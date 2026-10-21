"""
The limits set on the session running a provenance query (statement_timeout,
provsql.max_memory, temp_file_limit, parallel workers).
"""

import pytest
from psycopg import AsyncConnection

from ap_explanation.errors import QueryLimitExceededError
from ap_explanation.internal.explainer.noop_explainer import NoOpExplainer
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repository.provenance import ProvenanceRepository, QueryLimits
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.semiring import DbSemiring
from tests.conftest import TestSchema


async def _service(conn: AsyncConnection, limits: QueryLimits) -> ProvenanceService:
    repo = await ProvenanceRepository.create(conn, SqlRewriter(), limits)
    return ProvenanceService(repo, NoOpExplainer())


def test_unset_limits_are_left_out():
    limits = QueryLimits(statement_timeout="1s", provsql_max_memory=None, temp_file_limit=None)
    assert limits.settings() == {
        "statement_timeout": "1s",
        "max_parallel_workers_per_gather": "0",
    }


@pytest.mark.asyncio
async def test_ko_statement_timeout(
    db_connection: AsyncConnection, why_semiring: DbSemiring, test_schema: TestSchema
):
    """A query running past statement_timeout fails with a clear error."""
    service = await _service(db_connection, QueryLimits(statement_timeout="200ms"))
    await service.annotate_dataset(test_schema.table, test_schema.schema, [why_semiring])

    table = f"{test_schema.schema}.{test_schema.table}"
    with pytest.raises(QueryLimitExceededError) as exc_info:
        await service.compute_provenance(
            test_schema.schema, f"SELECT a.id FROM {table} a, {table} b", [why_semiring]
        )
    assert exc_info.value.limit == "statement_timeout"
    assert "statement_timeout (200ms)" in str(exc_info.value)

    # The connection is still usable afterwards
    rows = await service.compute_provenance(
        test_schema.schema, f"SELECT * FROM {table} LIMIT 1", [why_semiring]
    )
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_limits_are_local_to_the_query(
    db_connection: AsyncConnection, why_semiring: DbSemiring, test_schema: TestSchema
):
    """The limits revert once the query ends, so annotation is not bounded by them."""
    cursor = await db_connection.execute("SHOW statement_timeout")
    before = (await cursor.fetchone())[0]

    service = await _service(db_connection, QueryLimits(statement_timeout="5min"))
    await service.annotate_dataset(test_schema.table, test_schema.schema, [why_semiring])
    await service.compute_provenance(
        test_schema.schema,
        f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 1",
        [why_semiring],
    )

    cursor = await db_connection.execute("SHOW statement_timeout")
    assert (await cursor.fetchone())[0] == before
