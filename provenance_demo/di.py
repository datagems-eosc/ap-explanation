from contextlib import asynccontextmanager
from os import getenv
from typing import AsyncGenerator

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


def get_provenance_service(conn: AsyncConnection = Depends(get_db_conn)) -> ProvenanceService:
    """
    Create and configure the ProvenanceService with all its dependencies.

    This factory function:
    1. Creates the "why" semiring configuration
    2. Creates a SqlRewriter for query transformation
    3. Creates a ProvenanceRepository with both semiring and SqlRewriter
    4. Assembles the ProvenanceService
    """
    # Create the "why" semiring configuration
    # why_semiring = DbSemiring(
    #     name="why",
    #     retrieval_function="whyprov_now",
    #     aggregate_function="aggregation_formula",
    #     mapping_table="why_mapping",
    #     mappingStrategy=CtidMapping()
    # )

    formula_semiring = DbSemiring(
        name="formula",
        retrieval_function="formula",
        aggregate_function="aggregation_formula",
        mapping_table="formula_mapping",
        mappingStrategy=CtidMapping()
    )

    # Create the SQL rewriter
    sql_rewriter = SqlRewriter()

    # Create the unified provenance repository
    provenance_repo = ProvenanceRepository(
        conn, formula_semiring, sql_rewriter)

    # Assemble and return the service
    return ProvenanceService(provenance_repo)
