import asyncio
from pathlib import Path
from typing import List

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

from provenance_demo.internal.sql_rewriter import SqlRewriter
from provenance_demo.repository.mapping.ctid_mapping import CtidMapping
from provenance_demo.repository.provenance import ProvenanceRepository
from provenance_demo.semirings import semirings
from provenance_demo.services.provenance import ProvenanceService
from provenance_demo.types.semiring import DbSemiring


@pytest.fixture(scope="session")
def postgres_container():
    """
    PostgreSQL testcontainer with ProvSQL extension.
    Uses the custom postgres-provsql image from the dependencies folder.
    """
    # Build the custom Postgres image with ProvSQL
    postgres = PostgresContainer(
        image="postgres-provsql:latest",
        username="provdemo",
        password="provdemo",
        dbname="provenance_test"
    )
    postgres.start()
    yield postgres
    postgres.stop()


# @pytest.fixture(scope="session")
# def db_connection_string(postgres_container):
#     """Get the database connection string from the test container."""
#     return postgres_container.get_connection_url()


# @pytest.fixture(scope="session")
# async def db_connection(db_connection_string):
#     """Create an async database connection for testing."""
#     conn = await psycopg.AsyncConnection.connect(
#         db_connection_string,
#         autocommit=False
#     )

#     # Load initial schema and data
#     fixtures_dir = Path(__file__).parent.parent / "fixtures"

#     # Load the main schema
#     schema_file = fixtures_dir / "postgres-seed" / "01_mathe_pgsql.sql"
#     if schema_file.exists():
#         with open(schema_file, 'r') as f:
#             await conn.execute(f.read())

#     # Load annotations setup
#     annotations_file = fixtures_dir / "02_setup_mathe_annotations.sql"
#     if annotations_file.exists():
#         with open(annotations_file, 'r') as f:
#             await conn.execute(f.read())

#     # Load semiring setup
#     semiring_file = fixtures_dir / "postgres-seed" / "03_setup_semiring_parallel.sql"
#     if semiring_file.exists():
#         with open(semiring_file, 'r') as f:
#             await conn.execute(f.read())

#     await conn.commit()

#     yield conn

#     await conn.close()


@pytest.fixture
async def provenance_repository(db_connection, sql_rewriter: SqlRewriter):
    return ProvenanceRepository(db_connection, sql_rewriter)


@pytest.fixture
async def provenance_service(provenance_repository: ProvenanceRepository):
    return ProvenanceService(provenance_repository)


@pytest.fixture(scope="session")
def sql_rewriter():
    """SQL rewriter for testing query transformations."""
    return SqlRewriter()


@pytest.fixture(scope="session")
def all_semirings() -> List[DbSemiring]:
    """Why provenance semiring configuration for testing."""
    return semirings


@pytest.fixture(scope="session")
def why_semiring(all_semirings: List[DbSemiring]) -> DbSemiring:
    """Why provenance semiring configuration for testing."""
    return next(s for s in all_semirings if s.name == "why")


@pytest.fixture(scope="session")
def formula_semiring(all_semirings: List[DbSemiring]) -> DbSemiring:
    """How provenance semiring configuration for testing."""
    return next(s for s in semirings if s.name == "formula")
