import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable

from dotenv import load_dotenv
from fastapi import FastAPI
from psycopg import AsyncConnection, OperationalError
from psycopg_pool import AsyncConnectionPool

from ap_explanation.errors.exceptions import DatabaseNotFoundError
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repository.provenance import ProvenanceRepository
from ap_explanation.semirings import semirings
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.semiring import DbSemiring

load_dotenv()


@asynccontextmanager
async def container_lifespan(_: FastAPI):
    """
    Lifespan context manager for the FastAPI application.
    Connection pools are created and closed per-AP processing.
    """
    yield


@asynccontextmanager
async def get_dynamic_db_conn(connection_string: str) -> AsyncGenerator[AsyncConnection, None]:
    """
    Creates a temporary database connection pool, yields a connection, then closes the pool.
    This ensures the connection pool is cleaned up after AP processing completes.

    Args:
        connection_string: PostgreSQL connection string from AP
    """
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
