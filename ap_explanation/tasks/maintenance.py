"""
Celery task reclaiming ProvSQL's circuit store.

ProvSQL writes the circuit of every provenance query to its store
(``provsql_*.mmap`` in the database directory) and keeps it after the query
ends, failed or stopped ones included: the store only grows, and ProvSQL's
store worker holds it in memory. ``provsql.circuit_cleanup()`` keeps the gates
a token stored in the database still references and rewrites the files. Like
``DROP DATABASE``, it refuses to run while another session is connected to the
database, so it takes the database's explain lock first: no explain task holds
a connection to it meanwhile. Other clients of the database must disconnect.
"""
import logging

from psycopg import AsyncConnection

from ap_explanation.celery_app import celery_app
from ap_explanation.di import find_database, locked
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repositories.provsql import ProvSqlRepository
from ap_explanation.settings import settings
from ap_explanation.tasks.explain import _run_in_thread

logger = logging.getLogger(__name__)

CIRCUIT_CLEANUP_WAIT = settings.circuit_cleanup_wait


async def _do_circuit_cleanup(db_name: str) -> dict:
    async with await AsyncConnection.connect(await find_database(db_name), autocommit=True) as conn:
        repository = await ProvSqlRepository.create(conn, SqlRewriter())
        return await repository.cleanup_circuits(CIRCUIT_CLEANUP_WAIT)


@celery_app.task(bind=True, name="ap_explanation.tasks.maintenance.circuit_cleanup_task")
def circuit_cleanup_task(self, db_name: str) -> dict:
    """Celery task: run ``provsql.circuit_cleanup()`` on *db_name*.

    Holds the per-database explain lock (``explain_lock:{db_name}``) so no
    explain task connects meanwhile, and retries for ``CIRCUIT_CLEANUP_WAIT``
    seconds while other sessions are connected.

    Returns:
        ProvSQL's counts before and after: ``gates_before``, ``gates_after``,
        ``wires_before``, ``wires_after``, ``extra_bytes_before``,
        ``extra_bytes_after``.
    """
    logger.info(f"[task:{self.request.id}] Acquiring the lock of '{db_name}' for circuit cleanup")
    with locked(db_name):
        res = _run_in_thread(_do_circuit_cleanup(db_name))
    logger.info(f"[task:{self.request.id}] Circuit cleanup of '{db_name}': {res}")
    return res
