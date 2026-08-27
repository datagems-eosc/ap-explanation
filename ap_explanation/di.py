import logging
import os
import threading
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path
from typing import AsyncGenerator, Callable, Optional

from dotenv import load_dotenv
from fastapi import FastAPI
from psycopg import AsyncConnection, OperationalError
from psycopg_pool import AsyncConnectionPool

from ap_explanation.errors.exceptions import DatabaseNotFoundError
from ap_explanation.internal.cache import CacheProvider, RedisCacheProvider
from ap_explanation.internal.distributed_lock import LockProvider, RedisLockProvider
from ap_explanation.internal.explainer import Explainer, ExplanationAgent, NoOpExplainer
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repository.provenance import ProvenanceRepository
from ap_explanation.semirings import semirings
from ap_explanation.services.authentication import Authentication
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.data_sources import DataSource
from ap_explanation.types.semiring import DbSemiring

load_dotenv()
logger = logging.getLogger(__name__)
REDIS_BROKER_URI = os.getenv("REDIS_BROKER_URI", "redis://redis:6379/0")
S3_MOUNT_PATH = os.getenv("S3_MOUNT_PATH", "/mnt/s3")
# When True (default), the FastAPI process spawns a Celery worker in a daemon
# thread so no separate worker process is needed. Set to 'false' when running
# dedicated standalone workers (e.g. via Docker) to avoid double-processing.
USE_EMBEDDED_CELERY_WORKER = os.getenv(
    "USE_EMBEDDED_CELERY_WORKER", "true").lower() == "true"
# Poor man singleton
lock_provider = RedisLockProvider(redis_url=REDIS_BROKER_URI)
cache_provider = RedisCacheProvider(redis_url=REDIS_BROKER_URI)


def get_lock_provider() -> LockProvider:
    return lock_provider


def get_cache_provider() -> CacheProvider:
    return cache_provider


def get_s3_mount_path() -> Path:
    return Path(S3_MOUNT_PATH)


def _start_celery_worker() -> threading.Thread:
    """Start an embedded Celery worker in a daemon thread."""
    from ap_explanation.celery_app import celery_app  # noqa: ensure tasks registered

    worker = celery_app.Worker(
        loglevel="INFO",
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


def get_explainer() -> Explainer:
    api_base = os.getenv("LLM_API_BASE")
    api_key = os.getenv("LLM_API_KEY", None)
    model = os.getenv("LLM_API_MODEL")
    ssl_verify = os.getenv("LLM_SSL_VERIFY", "true").lower() == "true"

    if not api_base:
        logger.warning(
            "LLM_API_BASE not set, NL explanations will be disabled for provenance results."
        )
        return NoOpExplainer()

    if not all([api_base, model]):
        raise ValueError(
            "Missing required environment variables for LLM explanation: LLM_API_BASE, LLM_API_MODEL"
        )

    raw_timeout = os.getenv("LLM_TIMEOUT")
    try:
        timeout = (
            float(raw_timeout) if raw_timeout else ExplanationAgent.DEFAULT_TIMEOUT
        )
    except ValueError:
        raise ValueError(
            f"LLM_TIMEOUT must be a number of seconds, got '{raw_timeout}'"
        ) from None
    if timeout <= 0:
        raise ValueError(
            f"LLM_TIMEOUT must be a positive number of seconds, got '{raw_timeout}'"
        )

    return ExplanationAgent(
        api_base, model, api_key, ssl_verify=ssl_verify, timeout=timeout
    )


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
async def create_connection_pool(connection_string: str) -> AsyncGenerator[AsyncConnection, None]:
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


@lru_cache(maxsize=1)
def get_authentication_service() -> Optional[Authentication]:
    """Return a JwtValidator configured from environment variables."""
    if not os.getenv("OIDC_ISSUER"):
        logger.warning("OIDC_ISSUER not set, authentication disabled")
        return None

    return Authentication(
        issuer=os.getenv("OIDC_ISSUER", ""),
        ttl=int(os.getenv("JWKS_TTL_SECONDS", "300")),
        client_id=os.getenv("OIDC_CLIENT_ID") or None,
        client_secret=os.getenv("OIDC_CLIENT_SECRET") or None,
        exchange_scope=os.getenv("OIDC_EXCHANGE_SCOPE"),
    )


def get_provenance_service_for_ap(data_source: DataSource) -> Callable[[], AsyncGenerator[ProvenanceService, None]]:
    """
    Factory function to create a provenance service dependency with dynamic database connection.
    The connection pool is created when the AP is processed and closed when processing completes.

    Args:
        data_source: The resolved data source from the Provenance AP.

    Returns:
        Dependency function for ProvenanceService that can be used in FastAPI routes

    Raises:
        DatabaseNotFoundError: If the database doesn't exist on either Postgres or Timescale
    """

    async def check_db_location(db_name: str) -> str:
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

        postgres_connection_string = f"postgresql://{user}:{password}@{postgres_host}:{postgres_port}/{db_name}"
        timescale_connection_string = f"postgresql://{user}:{password}@{timescale_host}:{timescale_port}/{db_name}"

        # Try Postgres first
        try:
            check_conn = await AsyncConnection.connect(postgres_connection_string)
            await check_conn.close()
            return postgres_connection_string
        except OperationalError:
            pass

        # Then try Timescale
        try:
            check_conn = await AsyncConnection.connect(timescale_connection_string)
            await check_conn.close()
            return timescale_connection_string
        except OperationalError:
            pass

        raise DatabaseNotFoundError(db_name)

    async def _provide_service() -> AsyncGenerator[ProvenanceService, None]:
        qs = await check_db_location(data_source.db_name)
        async with create_connection_pool(qs) as pool:
            # NOTE: Some data sources require set up
            async with data_source.seed_database(pool, get_s3_mount_path()):
                repo = await ProvenanceRepository.create(pool, SqlRewriter())
                yield ProvenanceService(repo, get_explainer())
            return

    return _provide_service
