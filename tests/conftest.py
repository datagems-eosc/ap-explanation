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

from ap_explanation.internal.explainer.noop_explainer import NoOpExplainer
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repository.provenance import ProvenanceRepository
from ap_explanation.semirings import semirings
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.semiring import DbSemiring


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

    with DockerImage(
        path=str(project_root),
        dockerfile_path="dependencies/postgres-provsql/Dockerfile",
        tag="testdb:latest",
        clean_up=False,
        buildargs={
            "FIXTURES_PATH": "fixtures/postgres-seed",
            # Pin explicitly: an unset arg used to fall through to the
            # Dockerfile default, so the suite validated against a different
            # ProvSQL than docker-compose runs.
            "PROVSQL_VERSION": "v1.12.0",
        },
    ) as image:
        with PostgresContainer(
            image=str(image), username="provdemo", password="provdemo", dbname="mathe"
        ) as postgres:
            print(postgres.get_logs())
            yield postgres


@pytest.fixture
def connstr(postgres_container: PostgresContainer):
    """Returns a factory that builds a psycopg connection string for the container, optionally overriding user/password."""

    def _build(user: str = "provdemo", password: str = "provdemo") -> str:
        qs = postgres_container.get_connection_url()
        parsed = urlparse(qs)
        scheme = parsed.scheme.split("+", 1)[0]  # strip +psycopg2 suffix
        netloc = f"{user}:{password}@{parsed.hostname}:{parsed.port}"
        return urlunparse(parsed._replace(scheme=scheme, netloc=netloc))

    return _build


@pytest_asyncio.fixture
async def db_pool(connstr) -> AsyncGenerator[AsyncConnectionPool]:
    """Provides a connection to the test database."""
    pool = AsyncConnectionPool(
        conninfo=connstr(),
        min_size=1,
        max_size=5,
    )
    await pool.open()
    yield pool  # type: ignore
    await pool.close()


@pytest_asyncio.fixture
async def db_connection(
    db_pool: AsyncConnectionPool,
) -> AsyncGenerator[AsyncConnection]:
    """
    Returns a database connection from the pool with autocommit enabled,
    matching the production behaviour in create_connection_pool.
    """
    async with db_pool.connection() as conn:
        await conn.set_autocommit(True)
        yield conn


@pytest_asyncio.fixture
async def provenance_repository(
    db_connection: AsyncConnection, sql_rewriter: SqlRewriter
):
    """
    Returns a ProvenanceRepository with semiring setup ensured.
    """
    return await ProvenanceRepository.create(db_connection, sql_rewriter)


@pytest.fixture
def provenance_service(provenance_repository: ProvenanceRepository):
    return ProvenanceService(provenance_repository, NoOpExplainer())


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
    return next(s for s in all_semirings if s.name == "formula")


@pytest.fixture(scope="session")
def boolexpr_semiring(all_semirings: List[DbSemiring]) -> DbSemiring:
    """Boolean-expression provenance semiring configuration for testing."""
    return next(s for s in all_semirings if s.name == "boolexpr")


@pytest.fixture(scope="session")
def how_semiring(all_semirings: List[DbSemiring]) -> DbSemiring:
    """How-provenance semiring configuration for testing."""
    return next(s for s in all_semirings if s.name == "how")


@pytest.fixture(scope="session")
def which_semiring(all_semirings: List[DbSemiring]) -> DbSemiring:
    """Which-provenance semiring configuration for testing."""
    return next(s for s in all_semirings if s.name == "which")


@pytest.fixture(
    scope="session",
    params=list(Path(__file__).parent.parent.glob("fixtures/explain_sql_query*.json")),
    ids=lambda p: p.stem,
)
def explain_sql_query_file(request) -> Path:
    """Parametrized fixture yielding each explain_sql_query*.json fixture file path."""
    return request.param


@pytest.fixture(autouse=True, scope="session")
def configure_celery_for_tests():
    """Override Celery to run tasks synchronously in-process using an in-memory backend.

    This avoids requiring a running Redis or worker process during tests.
    `task_always_eager=True` executes tasks inline; `task_eager_propagates=True`
    ensures exceptions raised inside tasks are re-raised to the caller.
    """
    from ap_explanation.celery_app import celery_app

    celery_app.conf.update(
        task_always_eager=True,
        task_eager_propagates=True,
        broker_url="memory://",
        result_backend="cache+memory://",
    )
    yield
    # Restore defaults so other session fixtures are not affected
    celery_app.conf.update(
        task_always_eager=False,
        task_eager_propagates=False,
    )
