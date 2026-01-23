from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable

from fastapi import Depends, FastAPI
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from provenance_demo.internal.sql_rewriter import SqlRewriter
from provenance_demo.repository.provenance import ProvenanceRepository
from provenance_demo.semirings import semirings
from provenance_demo.services.provenance import ProvenanceService
from provenance_demo.types.semiring import DbSemiring


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
    )
    await pool.open()

    try:
        async with pool.connection() as conn:
            yield conn
    finally:
        await pool.close()


async def get_semirings() -> list[DbSemiring]:
    return semirings


def get_provenance_service_for_ap(connection_string: str) -> Callable[[], AsyncGenerator[ProvenanceService, None]]:
    """
    Factory function to create a provenance service dependency with dynamic database connection.
    The connection pool is created when the AP is processed and closed when processing completes.

    Args:
        connection_string: Database connection string from AP

    Returns:
        Dependency function for ProvenanceService that can be used in FastAPI routes
    """

    async def _provide_service() -> AsyncGenerator[ProvenanceService, None]:
        async with get_dynamic_db_conn(connection_string) as conn:
            repo = ProvenanceRepository(conn, SqlRewriter())
            yield ProvenanceService(repo)

    return _provide_service
