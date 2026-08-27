"""
Celery tasks for the explain operation (annotate → compute provenance → remove annotation).

Since the provenance logic is async (uses psycopg async connections), each task
runs the coroutine in a dedicated thread so it gets its own event loop – this is
safe regardless of whether the caller already has a running loop (e.g. pytest-asyncio).
"""
import asyncio
import concurrent.futures
import logging
from typing import Optional

from ap_explanation.celery_app import celery_app
from ap_explanation.di import (
    get_cache_provider,
    get_lock_provider,
    get_provenance_service_for_ap,
)
from ap_explanation.internal.cache import RedisCacheProvider
from ap_explanation.semirings import semirings as all_semirings
from ap_explanation.types.provenance import Provenance
from ap_explanation.types.provenance_analytical_pattern import (
    ProvenanceAnalyticalPattern,
)

logger = logging.getLogger(__name__)


def _run_in_thread(coro):
    """
    Execute *coro* inside a brand-new event loop running in a dedicated thread.
    This avoids 'cannot run nested event loop' errors when the task is executed
    eagerly (task_always_eager=True) from within an async test.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, coro)
        return future.result()


async def _do_explain(
    ap_dict: dict,
    semiring_name: Optional[str] = None,
) -> list:
    """
    Core async logic: annotate tables → compute provenance → remove annotation.

    If *semiring_name* is ``None`` all configured semirings are used; otherwise
    only the requested one.
    """

    # Resolve semirings to use
    if semiring_name is not None:
        semiring = next(
            (s for s in all_semirings if s.name == semiring_name), None)
        if not semiring:
            available = ", ".join(s.name for s in all_semirings)
            raise ValueError(
                f"Semiring '{semiring_name}' not found. Available: {available}"
            )
        target_semirings = [semiring]
    else:
        target_semirings = all_semirings

    ap = ProvenanceAnalyticalPattern.model_validate(ap_dict)
    ds = ap.data_source
    query = ap.sql_operator.properties["query"]

    service_factory = get_provenance_service_for_ap(ds)

    async for service in service_factory():
        # 1. Annotate
        for table_name in ds.table_names:
            await service.annotate_dataset(table_name, ds.schema_name, target_semirings)

        # 2. Compute provenance
        derivations = await service.compute_provenance(ds.schema_name, query, target_semirings)

        # 3. Compute NL explanation (if enabled)
        explanation = await service.explain(ds.schema_name, query, derivations)

        # # 4. Remove annotation (see provsql issue #67 workaround)
        # for table_name in ds.table_names:
        #     await service.remove_annotation(table_name, ds.schema_name)

        prov = Provenance(derivations=derivations, explanation=explanation)

        # Serialize the provenance result to JSON for caching and eventual IPC back to the caller
        return prov.model_dump(mode="json")

    return []


@celery_app.task(bind=True, name="ap_explanation.tasks.explain.explain_task")
def explain_task(
    self,
    ap_dict: dict,
    semiring_name: Optional[str] = None,
) -> list:
    """Celery task: annotate + compute provenance + remove annotation.

    Acquires a per-database Redis lock (key ``explain_lock:{db_name}``) so that
    only one task runs at a time against a given *db_name*.

    Results are cached in Redis under a SHA-256 key derived from all input
    parameters.  A cache hit returns immediately without touching the database.
    The TTL is controlled by ``RedisCacheProvider.DEFAULT_TTL`` (default 1 h).
    """
    cache = get_cache_provider()
    cache_key = RedisCacheProvider.make_key(ap_dict, semiring_name)

    cached = cache.get(cache_key)
    if cached is not None:
        logger.info(
            f"[task:{self.request.id}] Cache hit for key '{cache_key}' — skipping DB work"
        )
        return cached

    # Parse the AP to determine the lock target (db_name) without doing any I/O.
    ap = ProvenanceAnalyticalPattern.model_validate(ap_dict)
    db_name = ap.data_source.db_name

    logger.info(
        f"[task:{self.request.id}] Explaining AP in db '{db_name}'"
        + (f" with semiring '{semiring_name}'" if semiring_name else " with all semirings")
    )

    # Acquire lock to ensure exclusive access to the database during the explain operation
    # TODO: We could consider finer-grained locks in the future, e.g. per-table instead of per-db, to allow more concurrency
    lock_provider = get_lock_provider()
    lock_key = f"explain_lock:{db_name}"

    logger.info(f"[task:{self.request.id}] Acquiring lock '{lock_key}'")
    with lock_provider.acquire(lock_key):
        logger.info(f"[task:{self.request.id}] Acquired lock '{lock_key}'")
        res = _run_in_thread(
            _do_explain(ap_dict, semiring_name)
        )
        logger.info(f"[task:{self.request.id}] Released lock '{lock_key}'")

    cache.set(cache_key, res)
    logger.debug(
        f"[task:{self.request.id}] Result cached under key '{cache_key}'")
    return res
