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


def test_aggregated_rows_reads_the_input_of_each_aggregate():
    """The rows going into an aggregate, not those of a subquery in its expressions."""
    plan = {
        "Node Type": "Aggregate", "Plans": [
            {"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Actual Rows": 1000, "Actual Loops": 3},
            {"Node Type": "Aggregate", "Parent Relationship": "InitPlan", "Plans": [
                {"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Actual Rows": 7, "Actual Loops": 1},
            ]},
        ],
    }
    from ap_explanation.repository.provenance import _aggregated_rows

    assert _aggregated_rows(plan) == [3000, 7]


@pytest.mark.asyncio
async def test_ko_too_many_aggregated_rows(
    db_connection: AsyncConnection, formula_semiring: DbSemiring, test_schema: TestSchema
):
    """An aggregate over more rows than max_aggregated_rows is refused before its provenance is built."""
    service = await _service(db_connection, QueryLimits(max_aggregated_rows=3))
    await service.annotate_dataset(test_schema.table, test_schema.schema, [formula_semiring])

    table = f"{test_schema.schema}.{test_schema.table}"
    with pytest.raises(QueryLimitExceededError) as exc_info:
        await service.compute_provenance(
            test_schema.schema, f"SELECT COUNT(*) FROM {table}", [formula_semiring]
        )
    assert exc_info.value.limit == "max_aggregated_rows"
    assert "rows" in exc_info.value.details

    # Under the limit, the same aggregate is explained
    service = await _service(db_connection, QueryLimits(max_aggregated_rows=1_000_000))
    rows = await service.compute_provenance(
        test_schema.schema, f"SELECT COUNT(*) FROM {table}", [formula_semiring]
    )
    assert len(rows) == 1
    # The check's SET LOCALs were undone: ProvSQL was active for the query
    assert rows[0].provenance[formula_semiring.name].expression


@pytest.mark.asyncio
async def test_ko_precheck_timeout(
    db_connection: AsyncConnection, formula_semiring: DbSemiring, test_schema: TestSchema
):
    """A query too slow even without provenance is refused, and the settings revert."""
    service = await _service(db_connection, QueryLimits(precheck_timeout="200ms"))
    await service.annotate_dataset(test_schema.table, test_schema.schema, [formula_semiring])

    table = f"{test_schema.schema}.{test_schema.table}"
    with pytest.raises(QueryLimitExceededError) as exc_info:
        await service.compute_provenance(
            test_schema.schema, f"SELECT COUNT(*) FROM {table} WHERE pg_sleep(1) IS NOT NULL", [formula_semiring]
        )
    assert exc_info.value.limit == "the plain query's statement_timeout"

    cursor = await db_connection.execute("SHOW provsql.active")
    assert (await cursor.fetchone())[0] == "on"


@pytest.mark.asyncio
async def test_ok_aggregate_formula_as_rows(
    db_connection: AsyncConnection, formula_semiring: DbSemiring, test_schema: TestSchema,
    two_rows_of_one_student: str,
):
    """
    An aggregate's formula is read as its first max_formula_rows rows: the
    aggregate with its number of terms, then one term per row, and the tuples
    of those terms only.
    """
    service = await _service(db_connection, QueryLimits(max_formula_rows=3))
    await service.annotate_dataset(test_schema.table, test_schema.schema, [formula_semiring])

    table = f"{test_schema.schema}.{test_schema.table}"
    async with db_connection.transaction():
        await db_connection.execute("SET LOCAL provsql.active = 0")
        count = (await (await db_connection.execute(f"SELECT COUNT(*) FROM {table}")).fetchone())[0]
    rows = await service.compute_provenance(test_schema.schema, f"SELECT COUNT(*) AS n FROM {table}", [formula_semiring])

    assert len(rows) == 1
    formula = rows[0].provenance[formula_semiring.name]
    lines = formula.expression.splitlines()
    assert len(lines) == 3
    assert lines[0].startswith("#1 = ") and f"({count} terms)" in lines[0]
    assert formula.truncated
    assert len(formula.data) == 2  # the tuples of the two terms read
    assert str(count) in str(rows[0].answer["n"])

    # A small aggregate is written in one row: its formula, as sr_formula gives it
    rows = await service.compute_provenance(
        test_schema.schema, f"SELECT COUNT(*) FROM {table} WHERE id IN ({two_rows_of_one_student})", [formula_semiring]
    )
    formula = rows[0].provenance[formula_semiring.name]
    assert not formula.truncated
    assert "#1" not in formula.expression
    assert len(formula.data) == 2
