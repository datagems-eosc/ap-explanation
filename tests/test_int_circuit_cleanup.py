"""
The maintenance task reclaiming ProvSQL's circuit store, which needs the
database to itself.
"""

import pytest
from psycopg import AsyncConnection

from ap_explanation.errors import DatabaseInUseError
from ap_explanation.internal.explainer.noop_explainer import NoOpExplainer
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.tasks import maintenance
from ap_explanation.types.semiring import DbSemiring
from tests.conftest import TestSchema


async def _explain(connstr: str, semiring: DbSemiring, test_schema: TestSchema, query: str):
    """Annotate and explain on a connection of its own, closed afterwards."""
    async with await AsyncConnection.connect(connstr, autocommit=True) as conn:
        service = await ProvenanceService.create(conn, SqlRewriter(), NoOpExplainer())
        await service.annotate_dataset(test_schema.table, test_schema.schema, [semiring])
        return await service.compute_provenance(test_schema.schema, query, [semiring])


@pytest.fixture
def cleanup_target(connstr, monkeypatch):
    """Point the cleanup at the test database, without waiting long."""

    async def find_database(_db_name: str) -> str:
        return connstr()

    monkeypatch.setattr(maintenance, "find_database", find_database)
    monkeypatch.setattr(maintenance, "CIRCUIT_CLEANUP_WAIT", 5)
    return connstr()


@pytest.mark.asyncio
async def test_ok_circuit_cleanup(
    cleanup_target: str, why_semiring: DbSemiring, test_schema: TestSchema
):
    table = f"{test_schema.schema}.{test_schema.table}"
    # A join leaves gates no stored token references
    await _explain(
        cleanup_target, why_semiring, test_schema,
        f"SELECT a.id FROM {table} a JOIN {table} b ON a.student_id = b.student_id LIMIT 50",
    )

    res = await maintenance._do_circuit_cleanup("mathe")
    assert res["gates_after"] < res["gates_before"]

    # Explanations still work afterwards
    rows = await _explain(
        cleanup_target, why_semiring, test_schema, f"SELECT * FROM {table} LIMIT 3"
    )
    assert all(row.provenance[why_semiring.name].data for row in rows)


@pytest.mark.asyncio
async def test_ko_circuit_cleanup_with_other_session(cleanup_target: str):
    async with await AsyncConnection.connect(cleanup_target, autocommit=True):
        with pytest.raises(DatabaseInUseError) as exc_info:
            await maintenance._do_circuit_cleanup("mathe")
    assert len(exc_info.value.sessions) == 1
