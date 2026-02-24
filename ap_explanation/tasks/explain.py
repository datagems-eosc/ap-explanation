"""
Celery tasks for the explain operation (annotate → compute provenance → remove annotation).

Since the provenance logic is async (uses psycopg async connections), each task
runs the coroutine in a dedicated thread so it gets its own event loop – this is
safe regardless of whether the caller already has a running loop (e.g. pytest-asyncio).
"""
import asyncio
import concurrent.futures
import logging
from json import loads
from typing import List, Optional

from ap_explanation.celery_app import celery_app

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
    db_name: str,
    tables_names: List[str],
    schema_name: str,
    query: str,
    semiring_name: Optional[str] = None,
) -> list:
    """
    Core async logic: annotate tables → compute provenance → remove annotation.

    If *semiring_name* is ``None`` all configured semirings are used; otherwise
    only the requested one.
    """
    from ap_explanation.di import get_provenance_service_for_ap
    from ap_explanation.semirings import semirings as all_semirings

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

    service_factory = get_provenance_service_for_ap(db_name)

    async for service in service_factory():
        # 1. Annotate
        for table_name in tables_names:
            await service.annotate_dataset(table_name, schema_name, target_semirings)

        # 2. Compute provenance
        prov = await service.compute_provenance(schema_name, query, target_semirings)

        # 3. Remove annotation (see provsql issue #67 workaround)
        for table_name in tables_names:
            await service.remove_annotation(table_name, schema_name)

        return loads(prov or "[]")

    return []


@celery_app.task(bind=True, name="ap_explanation.tasks.explain.explain_task")
def explain_task(
    self,
    db_name: str,
    tables_names: List[str],
    schema_name: str,
    query: str,
    semiring_name: Optional[str] = None,
) -> list:
    """Celery task: annotate + compute provenance + remove annotation."""
    logger.info(
        f"[task:{self.request.id}] Explaining tables {tables_names} in db '{db_name}'"
        + (f" with semiring '{semiring_name}'" if semiring_name else " with all semirings")
    )
    return _run_in_thread(
        _do_explain(db_name, tables_names, schema_name, query, semiring_name)
    )
