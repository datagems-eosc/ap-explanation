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
import asyncio
import logging
import time

from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row

from ap_explanation.celery_app import celery_app
from ap_explanation.di import find_database, get_lock_provider
from ap_explanation.errors import DatabaseInUseError, ProvSqlMissingError
from ap_explanation.settings import settings
from ap_explanation.tasks.explain import _run_in_thread

logger = logging.getLogger(__name__)

CIRCUIT_CLEANUP_WAIT = settings.circuit_cleanup_wait
_RETRY_INTERVAL = 2


async def _do_circuit_cleanup(db_name: str) -> dict:
    conn = await AsyncConnection.connect(await find_database(db_name), autocommit=True)
    try:
        deadline = time.monotonic() + CIRCUIT_CLEANUP_WAIT
        while True:
            try:
                # Must be the first statement of its transaction: autocommit
                cursor = await conn.cursor(row_factory=dict_row).execute(
                    "SELECT * FROM provsql.circuit_cleanup()"
                )
                return await cursor.fetchone() or {}
            except errors.ObjectInUse:
                if time.monotonic() >= deadline:
                    break
                await asyncio.sleep(_RETRY_INTERVAL)
            except (errors.UndefinedFunction, errors.InvalidSchemaName) as e:
                raise ProvSqlMissingError(
                    f"provsql.circuit_cleanup() is not available on '{db_name}' "
                    f"(ProvSQL >= 1.13 required): {e!s}"
                ) from e

        cursor = await conn.execute(
            "SELECT format('%s@%s (%s)', usename, coalesce(client_addr::text, 'local'),"
            " coalesce(nullif(application_name, ''), 'unnamed'))"
            " FROM pg_stat_activity WHERE datname = current_database()"
            " AND pid <> pg_backend_pid() AND backend_type = 'client backend'"
        )
        raise DatabaseInUseError(db_name, [row[0] for row in await cursor.fetchall()])
    finally:
        await conn.close()


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
    lock_key = f"explain_lock:{db_name}"
    logger.info(f"[task:{self.request.id}] Acquiring lock '{lock_key}' for circuit cleanup")
    with get_lock_provider().acquire(lock_key):
        res = _run_in_thread(_do_circuit_cleanup(db_name))
    logger.info(f"[task:{self.request.id}] Circuit cleanup of '{db_name}': {res}")
    return res
