"""
Celery tasks for the explain operation (annotate → compute provenance → remove annotation).

Since the provenance logic is async (uses psycopg async connections), each task
runs the coroutine in a dedicated thread so it gets its own event loop – this is
safe regardless of whether the caller already has a running loop (e.g. pytest-asyncio).
"""
import asyncio
import concurrent.futures
import logging
from collections.abc import Coroutine
from typing import Any

from celery import Task

from ap_explanation.celery_app import celery_app
from ap_explanation.di import (
    get_cache_provider,
    locked,
    provenance_service_for,
)
from ap_explanation.domains.provenance.provenance_analytical_pattern import (
    ProvenanceAnalyticalPattern,
)
from ap_explanation.internal.cache import RedisCacheProvider

logger = logging.getLogger(__name__)


def _run_in_thread(coro: Coroutine[Any, Any, dict[str, Any]]) -> dict[str, Any]:
    """
    Execute *coro* inside a brand-new event loop running in a dedicated thread.
    This avoids 'cannot run nested event loop' errors when the task is executed
    eagerly (task_always_eager=True) from within an async test.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, coro)
        return future.result()


async def _explain(
    ap: ProvenanceAnalyticalPattern,
    semiring_name: str | None,
    compute_probability: bool,
    cache_key: str,
    task_id: str,
) -> dict[str, Any]:
    """Explain *ap* under the lock of its database, unless a task did it while this one waited."""
    db_name = ap.data_source.db_name
    # TODO: We could consider finer-grained locks in the future, e.g. per-table instead of per-db, to allow more concurrency
    logger.info(f"[task:{task_id}] Acquiring the lock of '{db_name}'")
    async with locked(db_name):
        logger.info(f"[task:{task_id}] Acquired the lock of '{db_name}'")

        # Edge case: Explaining is a long-running operation.
        # So a task with the same key may have finished while this one waited.
        cache = get_cache_provider()
        res = cache.get(cache_key)
        if res is None:
            async with provenance_service_for(ap.data_source) as service:
                provenance = await service.explain_ap(ap, semiring_name, compute_probability)
            res = provenance.model_dump(mode="json")
            cache.set(cache_key, res)
    logger.info(f"[task:{task_id}] Released the lock of '{db_name}'")

    return res


@celery_app.task(bind=True, name="ap_explanation.tasks.explain.explain_task")
def explain_task(
    # Quoted: Celery's Task cannot be subscripted at runtime
    self: "Task[Any, Any]",  # noqa: UP037
    ap_dict: dict[str, Any],
    semiring_name: str | None = None,
    compute_probability: bool = False,
) -> dict[str, Any]:
    """
    Run the explanation of an AP in a dedicated thread.
    To prevent side effect, only one task can run at a time for a given database.
    The result is cached for 1 hour, so that subsequent tasks with the same AP and semiring can skip the DB work.

    :param ap_dict: The AP to explain. As dict because Celery tasks must be serializable.
    :param semiring_name: The semiring to use for the explanation. If None, all configured semirings are used.
    :param compute_probability: Whether to compute the probability of each result row. If True, tuple probabilities are read from each data node's probabilityColumn.

    :return: The provenance of the AP as a dict.
    """
    # 1 : Check the cache for a previous result. If found, return it immediately.
    cache = get_cache_provider()
    cache_key = RedisCacheProvider.make_key(
        ap_dict, semiring_name, compute_probability
    )
    cached = cache.get(cache_key)
    if cached is not None:
        logger.info(
            f"[task:{self.request.id}] Cache hit for key '{cache_key}' — skipping DB work"
        )
        return cached

    # 2 : If necessary, compute the result, putting a lock on the target db
    ap = ProvenanceAnalyticalPattern.model_validate(ap_dict)
    logger.info(
        f"[task:{self.request.id}] Explaining AP in db '{ap.data_source.db_name}'"
        + (f" with semiring '{semiring_name}'" if semiring_name else " with all semirings")
    )
    return _run_in_thread(
        _explain(ap, semiring_name, compute_probability, cache_key, str(self.request.id))
    )
