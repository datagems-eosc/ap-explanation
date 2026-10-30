"""
Semiring calls label rows through a temporary view over the annotated tables, so
annotating a table adds only ProvSQL's own column.
"""

import pytest
from psycopg import AsyncConnection

from ap_explanation.domains.provenance.semiring import DbSemiring
from ap_explanation.errors import TableNotAnnotatedError
from ap_explanation.repositories.provsql import references_in
from ap_explanation.services.provenance import ProvenanceService
from tests.conftest import TestSchema


async def _columns(conn: AsyncConnection, test_schema: TestSchema) -> set[str]:
    cursor = await conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
        (test_schema.schema, test_schema.table),
    )
    return {row[0] for row in await cursor.fetchall()}


async def _relations(conn: AsyncConnection, test_schema: TestSchema) -> set[str]:
    cursor = await conn.execute(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = %s AND c.relkind IN ('r', 'v')",
        (test_schema.schema,),
    )
    return {row[0] for row in await cursor.fetchall()}


async def _assert_resolves(
    service: ProvenanceService, semirings: list[DbSemiring], test_schema: TestSchema
) -> None:
    rows = await service.compute_provenance(
        test_schema.schema,
        f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 3",
        semirings,
    )
    assert rows
    for row in rows:
        for semiring in semirings:
            assert row.provenance[semiring.name].data, semiring.name


def test_a_value_is_not_a_reference():
    """An aggregate's formula holds the values it reads, here of a MIN(email)."""
    reference = "people@k" + "d501604ee4954d38badc7697a0cfbbd0"

    assert references_in(f"min({reference}*anna@kfe.org)") == [("people", reference)]


@pytest.mark.asyncio
async def test_annotating_adds_only_the_provsql_column(
    provenance_service: ProvenanceService,
    db_connection: AsyncConnection,
    all_semirings: list[DbSemiring],
    test_schema: TestSchema,
):
    """Annotating a table is enough for every semiring: none is chosen before the query."""
    columns = await _columns(db_connection, test_schema)
    relations = await _relations(db_connection, test_schema)

    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    assert await _columns(db_connection, test_schema) == columns | {"provsql"}
    assert await _relations(db_connection, test_schema) == relations
    await _assert_resolves(provenance_service, all_semirings, test_schema)


@pytest.mark.asyncio
async def test_a_query_labels_the_rows_of_all_its_tables(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)
    await provenance_service.annotate_dataset("last_assessment_levels", test_schema.schema)

    rows = await provenance_service.compute_provenance(
        test_schema.schema,
        f"SELECT a.id, l.last_level FROM {test_schema.table} a"
        " JOIN last_assessment_levels l ON a.student_id = l.student_id AND a.topic = l.topic"
        " ORDER BY a.id LIMIT 3",
        [why_semiring],
    )

    assert rows
    for row in rows:
        provenance = row.provenance[why_semiring.name]
        assert {table for table, _ in references_in(provenance.expression)} == {
            test_schema.table,
            "last_assessment_levels",
        }
        assert len(provenance.data) == 2


@pytest.mark.asyncio
async def test_a_copy_of_the_tokens_labels_nothing(
    provenance_service: ProvenanceService,
    db_connection: AsyncConnection,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """A table copied with its provsql column is not annotated: its rows are nobody's source."""
    table = f"{test_schema.schema}.{test_schema.table}"
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)
    async with db_connection.transaction():
        await db_connection.execute("SET LOCAL provsql.active = 0")
        await db_connection.execute(f"CREATE TABLE {table}_copy AS SELECT id, provsql FROM {table}")

    rows = await provenance_service.compute_provenance(
        test_schema.schema, f"SELECT * FROM {table} LIMIT 3", [why_semiring]
    )

    assert rows
    for row in rows:
        references = references_in(row.provenance[why_semiring.name].expression)
        assert references and {table for table, _ in references} == {test_schema.table}


@pytest.mark.asyncio
async def test_the_view_does_not_outlive_the_query(
    provenance_service: ProvenanceService,
    db_connection: AsyncConnection,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """Left on a pooled connection, it would stop other sessions dropping the tables it reads."""
    table = f"{test_schema.schema}.{test_schema.table}"
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    async def view() -> str | None:
        cursor = await db_connection.execute("SELECT to_regclass('pg_temp.apx_mapping')")
        return (await cursor.fetchone())[0]

    await provenance_service.compute_provenance(
        test_schema.schema, f"SELECT * FROM {table} LIMIT 3", [why_semiring]
    )
    assert await view() is None

    with pytest.raises(Exception, match="no_such_column"):
        await provenance_service.compute_provenance(
            test_schema.schema, f"SELECT no_such_column FROM {table}", [why_semiring]
        )
    assert await view() is None

    await _assert_resolves(provenance_service, [why_semiring], test_schema)
    await provenance_service.remove_annotation(test_schema.table, test_schema.schema)
    assert "provsql" not in await _columns(db_connection, test_schema)


@pytest.mark.asyncio
async def test_a_query_without_annotated_tables_is_refused(
    provenance_service: ProvenanceService,
    db_connection: AsyncConnection,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """A column named provsql does not make a table annotated."""
    table = f"{test_schema.schema}.{test_schema.table}"
    await db_connection.execute(f"CREATE TABLE {table}_lookalike (provsql uuid)")

    with pytest.raises(TableNotAnnotatedError):
        await provenance_service.compute_provenance(
            test_schema.schema, f"SELECT * FROM {table} LIMIT 3", [why_semiring]
        )
