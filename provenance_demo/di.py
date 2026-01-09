from contextlib import asynccontextmanager
from os import getenv
from typing import AsyncGenerator

from fastapi import Depends, FastAPI
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from provenance_demo.services.provenance import ProvenanceService

POSTGRES_HOST = getenv("POSTGRES_HOST")
POSTGRES_DB = getenv("POSTGRES_DB")
POSTGRES_USER = getenv("POSTGRES_USER")
POSTGRES_PASSWORD = getenv("POSTGRES_PASSWORD")


pool: AsyncConnectionPool


@asynccontextmanager
async def container_lifespan(_: FastAPI):
    """
    Lifespan context manager to setup and teardown the database connection pool.
    This ties the pool's lifecycle to that of the FastAPI application and prevents connection leaks.
    """
    global pool
    pool = AsyncConnectionPool(
        conninfo=(
            f"host={POSTGRES_HOST} "
            f"dbname={POSTGRES_DB} "
            f"user={POSTGRES_USER} "
            f"password={POSTGRES_PASSWORD}"
        ),
        min_size=1,
        max_size=5,
    )
    await pool.open()

    yield

    await pool.close()


async def get_db_conn() -> AsyncGenerator[AsyncConnection, None]:
    """
    Returns a database connection from the pool.
    """
    async with pool.connection() as conn:
        yield conn


def get_provenance_service(conn: AsyncConnection = Depends(get_db_conn)) -> ProvenanceService:
    return ProvenanceService(conn)
