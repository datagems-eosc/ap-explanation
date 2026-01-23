from contextlib import asynccontextmanager
from os import getenv
from typing import AsyncGenerator, List

from fastapi import Depends, FastAPI
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from provenance_demo.internal.sql_rewriter import SqlRewriter
from provenance_demo.repository.mapping.ctid_mapping import CtidMapping
from provenance_demo.repository.provenance import ProvenanceRepository
from provenance_demo.services.provenance import ProvenanceService
from provenance_demo.types.semiring import DbSemiring

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


async def get_sql_rewriter() -> SqlRewriter:
    return SqlRewriter()


async def get_semirings() -> List[DbSemiring]:
    return [
        DbSemiring(
            name="formula",
            retrieval_function="formula",
            aggregate_function="aggregation_formula",
            mapping_table="formula_mapping",
            mappingStrategy=CtidMapping()
        ),
        DbSemiring(
            name="why",
            retrieval_function="whyprov_now",
            aggregate_function="aggregation_formula",
            mapping_table="why_mapping",
            mappingStrategy=CtidMapping()
        )
    ]


def get_provenance_repo(conn: AsyncConnection = Depends(get_db_conn), sql_rewriter: SqlRewriter = Depends(get_sql_rewriter)) -> ProvenanceRepository:
    return ProvenanceRepository(conn, sql_rewriter)


def get_provenance_service(repo: ProvenanceRepository = Depends(get_provenance_repo)) -> ProvenanceService:
    return ProvenanceService(repo)
