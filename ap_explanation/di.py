import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable

from dotenv import load_dotenv
from fastapi import FastAPI
from psycopg import AsyncConnection, OperationalError
from psycopg_pool import AsyncConnectionPool

from ap_explanation.errors.exceptions import DatabaseNotFoundError
from ap_explanation.internal.distributed_lock import LockProvider, RedisLockProvider
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repository.provenance import ProvenanceRepository
from ap_explanation.semirings import semirings
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.semiring import DbSemiring

load_dotenv()
logger = logging.getLogger(__name__)
REDIS_BROKER_URI = os.getenv("REDIS_BROKER_URI", "redis://redis:6379/0")
# When True (default), the FastAPI process spawns a Celery worker in a daemon
# thread so no separate worker process is needed. Set to 'false' when running
# dedicated standalone workers (e.g. via Docker) to avoid double-processing.
USE_EMBEDDED_CELERY_WORKER = os.getenv(
    "USE_EMBEDDED_CELERY_WORKER", "true").lower() == "true"
# Poor man singleton
lock_provider = RedisLockProvider(redis_url=REDIS_BROKER_URI)


def get_lock_provider() -> LockProvider:
    return lock_provider


def _start_celery_worker() -> threading.Thread:
    """Start an embedded Celery worker in a daemon thread."""
    from ap_explanation.celery_app import celery_app  # noqa: ensure tasks registered

    worker = celery_app.Worker(
        loglevel="DEBUG",
        concurrency=2,
        pool="threads",
    )

    thread = threading.Thread(
        target=worker.start,
        daemon=True,
        name="celery-worker"
    )

    thread.start()
    logger.info("Embedded Celery worker started")
    return thread


@asynccontextmanager
async def container_lifespan(_: FastAPI):
    """
    Lifespan context manager for the FastAPI application.

    Conditionally starts an embedded Celery worker in a daemon thread based on
    the ``USE_EMBEDDED_CELERY_WORKER`` environment variable (default: ``true``).
    Set it to ``false`` when running dedicated standalone workers so the API
    process does not also consume tasks.
    """
    if USE_EMBEDDED_CELERY_WORKER:
        _start_celery_worker()
    else:
        logger.info(
            "Embedded Celery worker disabled (USE_EMBEDDED_CELERY_WORKER=false)")
    yield
    # Celery worker runs in a daemon thread – it will be terminated when the
    # process exits. Explicit stop is not needed but we log for visibility.
    if USE_EMBEDDED_CELERY_WORKER:
        logger.info("Shutting down embedded Celery worker")


@asynccontextmanager
async def get_dynamic_db_conn(connection_string: str) -> AsyncGenerator[AsyncConnection, None]:
    """
    Validates the connection string by opening a direct connection first, then creates a
    temporary database connection pool, yields a connection, and closes the pool afterwards.
    This ensures the connection pool is cleaned up after AP processing completes.

    Raises OperationalError immediately if the database does not exist, rather than
    letting the pool silently retry in the background.

    Args:
        connection_string: PostgreSQL connection string from AP
    """
    # Validate eagerly: raises OperationalError immediately if the DB doesn't exist
    check_conn = await AsyncConnection.connect(connection_string)
    await check_conn.close()

    pool = AsyncConnectionPool(
        conninfo=connection_string,
        min_size=1,
        max_size=5,
        open=False
    )

    try:
        await pool.open()
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            yield conn
    finally:
        await pool.close()


async def get_semirings() -> list[DbSemiring]:
    return semirings


def get_provenance_service_for_ap(db_name: str) -> Callable[[], AsyncGenerator[ProvenanceService, None]]:
    """
    Factory function to create a provenance service dependency with dynamic database connection.
    The connection pool is created when the AP is processed and closed when processing completes.
    Tries to connect to the Postgres instance first, then falls back to Timescale if the database
    doesn't exist on Postgres.

    Args:
        db_name: Database name to connect to

    Returns:
        Dependency function for ProvenanceService that can be used in FastAPI routes

    Raises:
        DatabaseNotFoundError: If the database doesn't exist on either Postgres or Timescale
    """

    async def _provide_service() -> AsyncGenerator[ProvenanceService, None]:
        # Get connection parameters from environment variables
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        postgres_host = os.getenv("POSTGRES_HOST")
        postgres_port = os.getenv("POSTGRES_PORT", "5432")
        timescale_host = os.getenv("POSTGRES_TIMESCALE_HOST")
        timescale_port = os.getenv("POSTGRES_TIMESCALE_PORT", "5433")

        if not all([user, password, postgres_host]):
            raise ValueError(
                "Missing required environment variables: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST"
            )

        # Try Postgres instance first
        postgres_connection_string = f"postgresql://{user}:{password}@{postgres_host}:{postgres_port}/{db_name}"

        try:
            async with get_dynamic_db_conn(postgres_connection_string) as conn:
                repo = ProvenanceRepository(conn, SqlRewriter())
                await repo.ensure_semiring_setup()
                yield ProvenanceService(repo)
                return
        except OperationalError:
            # Database doesn't exist on Postgres, try Timescale
            pass

        # Try Timescale instance
        timescale_connection_string = f"postgresql://{user}:{password}@{timescale_host}:{timescale_port}/{db_name}"

        try:
            async with get_dynamic_db_conn(timescale_connection_string) as conn:
                repo = ProvenanceRepository(conn, SqlRewriter())
                await repo.ensure_semiring_setup()
                yield ProvenanceService(repo)
                return
        except OperationalError:
            # Database doesn't exist on either instance
            raise DatabaseNotFoundError(db_name)

    return _provide_service
