"""
Every semiring shares one mapping table per table, and one view over them per
schema. Earlier versions created one identical table per semiring and a view per
semiring; databases annotated by them keep working, and are migrated when
annotated again.
"""

import pytest
from psycopg import AsyncConnection

from ap_explanation.domains.semiring import DbSemiring
from ap_explanation.repositories.annotation import AnnotationRepository
from ap_explanation.services.provenance import ProvenanceService
from tests.conftest import TestSchema


async def _mapping_tables(conn: AsyncConnection, test_schema: TestSchema) -> set[str]:
    cursor = await conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s AND tablename LIKE %s",
        (test_schema.schema, f"{test_schema.table}\\_prov%"),
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


@pytest.mark.asyncio
async def test_all_semirings_share_one_mapping(
    provenance_service: ProvenanceService,
    db_connection: AsyncConnection,
    all_semirings: list[DbSemiring],
    test_schema: TestSchema,
):
    """Annotating a table is enough for every semiring: none is chosen before the query."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    assert await _mapping_tables(db_connection, test_schema) == {f"{test_schema.table}_provkey"}
    await _assert_resolves(provenance_service, all_semirings, test_schema)


@pytest.mark.asyncio
async def test_per_semiring_tables_keep_working_then_migrate(
    provenance_service: ProvenanceService,
    annotation_repository: AnnotationRepository,
    db_connection: AsyncConnection,
    why_semiring: DbSemiring,
    how_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """
    A table annotated by an earlier version, with a maintained mapping per
    semiring, is still explained as is, and annotating it again leaves one
    shared mapping table.
    """
    semirings = [why_semiring, how_semiring]
    table = test_schema.table
    await provenance_service.annotate_dataset(table, test_schema.schema)

    # Wind back to what the earlier versions left: one maintained mapping per
    # semiring, and a relation per semiring over them, a view or a snapshot.
    await db_connection.execute(f"SET search_path TO {test_schema.schema}, public, provsql")
    await db_connection.execute(
        f"ALTER TABLE {table}_provkey RENAME TO {table}_provwhy"
    )
    await db_connection.execute(
        "SELECT create_provenance_mapping(%s, %s, 'provsql_ref', false, true)",
        (f"{table}_provhow", table),
    )
    await annotation_repository._rebuild_mapping_view(test_schema.schema)
    await db_connection.execute(f"CREATE VIEW why_mapping AS TABLE {table}_provwhy")
    await db_connection.execute(f"CREATE TABLE how_mapping AS TABLE {table}_provhow")
    assert await _mapping_tables(db_connection, test_schema) == {
        f"{table}_provwhy",
        f"{table}_provhow",
    }

    await _assert_resolves(provenance_service, semirings, test_schema)

    newly = await provenance_service.annotate_dataset(table, test_schema.schema)
    assert newly is False  # renamed, not rebuilt
    assert await _mapping_tables(db_connection, test_schema) == {f"{table}_provkey"}
    cursor = await db_connection.execute("SELECT to_regclass('why_mapping'), to_regclass('how_mapping')")
    assert await cursor.fetchone() == (None, None)

    # Still maintained after the rename: provenance_guard maps new rows
    cursor = await db_connection.execute(
        "SELECT count(*) FROM provsql.provenance_mapping_registry"
        " WHERE mapping = to_regclass(%s)::oid",
        (f"{test_schema.schema}.{table}_provkey",),
    )
    assert (await cursor.fetchone())[0] == 1

    await _assert_resolves(provenance_service, semirings, test_schema)
