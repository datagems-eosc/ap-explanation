from dataclasses import dataclass
from pathlib import Path
from typing import AsyncGenerator, List
from urllib.parse import urlparse, urlunparse

import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.image import DockerImage
from testcontainers.postgres import PostgresContainer

from provenance_demo.internal.sql_rewriter import SqlRewriter
from provenance_demo.repository.provenance import ProvenanceRepository
from provenance_demo.semirings import semirings
from provenance_demo.services.provenance import ProvenanceService
from provenance_demo.types.semiring import DbSemiring


@dataclass
class TestSchema:
    table: str = "assessment"
    schema: str = "mathe"


@pytest.fixture(scope="session")
def test_schema() -> TestSchema:
    return TestSchema()


@pytest.fixture(scope="function")
def postgres_container():
    # Get the project root directory (parent of tests/)
    project_root = Path(__file__).parent.parent
    fixtures_path = project_root / "fixtures" / "postgres-seed"

    if not fixtures_path.exists():
        raise FileNotFoundError(
            f"Fixtures path does not exist: {fixtures_path}")

    with DockerImage(path="dependencies/postgres-provsql", tag="testdb:latest", clean_up=False) as image:
        with PostgresContainer(
            image=str(image),
            username="provdemo",
            password="provdemo",
            dbname="mathe"
        ).with_volume_mapping(
            host=str(fixtures_path),
            container="/docker-entrypoint-initdb.d",
            mode="ro"
        ) as postgres:
            yield postgres


@pytest_asyncio.fixture
async def db_pool(postgres_container: PostgresContainer) -> AsyncGenerator[AsyncConnectionPool]:
    """Provides a connection to the test database."""
    qs = postgres_container.get_connection_url()
    parsed = urlparse(qs)
    scheme = parsed.scheme.split("+", 1)[0]  # remove +psycopg2
    qs = urlunparse(parsed._replace(scheme=scheme))

    pool = AsyncConnectionPool(
        conninfo=qs,
        min_size=1,
        max_size=5,
    )
    await pool.open()
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def db_connection(db_pool: AsyncConnectionPool) -> AsyncGenerator[AsyncConnection]:
    """
    Returns a database connection from the pool.
    """
    async with db_pool.connection() as conn:
        yield conn


@pytest_asyncio.fixture
async def provenance_repository(db_connection: AsyncConnection, sql_rewriter: SqlRewriter):
    """
    Returns a ProvenanceRepository with semiring setup ensured.
    """
    repo = ProvenanceRepository(db_connection, sql_rewriter)
    await repo.ensure_semiring_setup()
    return repo


@pytest.fixture
def provenance_service(provenance_repository: ProvenanceRepository):
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
