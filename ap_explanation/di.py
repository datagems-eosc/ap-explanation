import logging
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache

from fastapi import FastAPI
from psycopg import AsyncConnection, OperationalError
from psycopg.conninfo import make_conninfo
from psycopg_pool import AsyncConnectionPool

from ap_explanation.celery_app import celery_app
from ap_explanation.errors.exceptions import DatabaseNotFoundError
from ap_explanation.internal.cache import CacheProvider, RedisCacheProvider
from ap_explanation.internal.distributed_lock import LockProvider, RedisLockProvider, RedisRowBudget
from ap_explanation.internal.explainer import Explainer, ExplanationAgent, NoOpExplainer
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.semirings import semirings
from ap_explanation.services.authentication import Authentication
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.settings import QueryLimits, settings
from ap_explanation.types.data_sources import DataSource
from ap_explanation.types.semiring import DbSemiring

logger = logging.getLogger(__name__)
# Poor man singleton
lock_provider = RedisLockProvider(redis_url=settings.redis_broker_uri)
row_budget = RedisRowBudget(redis_url=settings.redis_broker_uri)
cache_provider = RedisCacheProvider(redis_url=settings.redis_broker_uri)
# Read once: they do not change while the process runs.
query_limits = QueryLimits()


def get_lock_provider() -> LockProvider:
    return lock_provider


def get_cache_provider() -> CacheProvider:
    return cache_provider


def _start_celery_worker() -> threading.Thread:
    """Start an embedded Celery worker in a daemon thread."""
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
    if not settings.llm_api_base:
        logger.warning(
            "LLM_API_BASE not set, NL explanations will be disabled for provenance results."
        )
        return NoOpExplainer()

    if not settings.llm_api_model:
        raise ValueError(
            "Missing required environment variables for LLM explanation: LLM_API_BASE, LLM_API_MODEL"
        )

    return ExplanationAgent(
        settings.llm_api_base,
        settings.llm_api_model,
        settings.llm_api_key,
        ssl_verify=settings.llm_ssl_verify,
        timeout=settings.llm_timeout,
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
    if settings.use_embedded_celery_worker:
        _start_celery_worker()
    else:
        logger.info(
            "Embedded Celery worker disabled (USE_EMBEDDED_CELERY_WORKER=false)")
    yield
    # Celery worker runs in a daemon thread – it will be terminated when the
    # process exits. Explicit stop is not needed but we log for visibility.
    if settings.use_embedded_celery_worker:
        logger.info("Shutting down embedded Celery worker")


@asynccontextmanager
async def create_connection_pool(connection_string: str) -> AsyncGenerator[AsyncConnection]:
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
def get_authentication_service() -> Authentication | None:
    """Return a JwtValidator configured from environment variables."""
    if not settings.oidc_issuer:
        logger.warning("OIDC_ISSUER not set, authentication disabled")
        return None

    return Authentication(
        issuer=settings.oidc_issuer,
        ttl=settings.jwks_ttl_seconds,
        client_id=settings.oidc_client_id,
        client_secret=settings.oidc_client_secret,
        exchange_scope=settings.oidc_exchange_scope,
    )


async def find_database(db_name: str) -> str:
    """
    Connection string of *db_name*, on Postgres or else on Timescale.

    Raises:
        DatabaseNotFoundError: If the database doesn't exist on either Postgres or Timescale
    """
    if not all([settings.postgres_user, settings.postgres_password, settings.postgres_host]):
        raise ValueError(
            "Missing required environment variables: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST"
        )

    # Postgres first, then Timescale if configured. make_conninfo quotes
    # db_name, which comes from the request: pasted into a URI, a name such
    # as 'db?host=elsewhere' would send the credentials to another server.
    candidates = [(settings.postgres_host, settings.postgres_port)]
    if settings.postgres_timescale_host:
        candidates.append((settings.postgres_timescale_host, settings.postgres_timescale_port))

    for host, port in candidates:
        connection_string = make_conninfo(
            host=host,
            port=port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            dbname=db_name,
        )
        try:
            check_conn = await AsyncConnection.connect(connection_string)
            await check_conn.close()
            return connection_string
        except OperationalError:
            pass

    raise DatabaseNotFoundError(db_name)


@asynccontextmanager
async def provenance_service_for(data_source: DataSource) -> AsyncGenerator[ProvenanceService]:
    """
    The provenance service of a data source, with a dynamic database connection.
    The connection pool is created on entry and closed on exit.

    Args:
        data_source: The resolved data source from the Provenance AP.

    Raises:
        DatabaseNotFoundError: If the database doesn't exist on either Postgres or Timescale
    """
    qs = await find_database(data_source.db_name)
    async with create_connection_pool(qs) as pool:
        # NOTE: Some data sources require set up
        async with data_source.seed_database(pool, settings.s3_mount_path):
            yield await ProvenanceService.create(pool, SqlRewriter(), get_explainer(), query_limits, row_budget)
