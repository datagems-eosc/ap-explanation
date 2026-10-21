"""
Every semiring using the same mapping strategy shares one mapping table per
table. Earlier versions created one identical table per semiring; databases
annotated by them keep working, and are migrated when annotated again.
"""

import pytest
from psycopg import AsyncConnection

from ap_explanation.repository.provenance import ProvenanceRepository
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.semiring import DbSemiring
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
async def test_all_semirings_share_one_mapping_table(
    provenance_service: ProvenanceService,
    db_connection: AsyncConnection,
    all_semirings: list[DbSemiring],
    test_schema: TestSchema,
):
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, all_semirings)

    assert await _mapping_tables(db_connection, test_schema) == {f"{test_schema.table}_provkey"}
    await _assert_resolves(provenance_service, all_semirings, test_schema)


@pytest.mark.asyncio
async def test_per_semiring_tables_keep_working_then_migrate(
    provenance_service: ProvenanceService,
    provenance_repository: ProvenanceRepository,
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
    await provenance_service.annotate_dataset(table, test_schema.schema, semirings)

    # Wind back to what the earlier version left: one maintained mapping per
    # semiring, and union views over them.
    await db_connection.execute(f"SET search_path TO {test_schema.schema}, public, provsql")
    await db_connection.execute(
        f"ALTER TABLE {table}_provkey RENAME TO {table}_provwhy"
    )
    await db_connection.execute(
        "SELECT create_provenance_mapping(%s, %s, 'provsql_ref', false, true)",
        (f"{table}_provhow", table),
    )
    for semiring in semirings:
        await provenance_repository._rebuild_union_mapping(test_schema.schema, semiring)
    assert await _mapping_tables(db_connection, test_schema) == {
        f"{table}_provwhy",
        f"{table}_provhow",
    }

    await _assert_resolves(provenance_service, semirings, test_schema)

    newly = await provenance_service.annotate_dataset(table, test_schema.schema, semirings)
    assert newly is False  # renamed, not rebuilt
    assert await _mapping_tables(db_connection, test_schema) == {f"{table}_provkey"}

    # Still maintained after the rename: provenance_guard maps new rows
    cursor = await db_connection.execute(
        "SELECT count(*) FROM provsql.provenance_mapping_registry"
        " WHERE mapping = to_regclass(%s)::oid",
        (f"{test_schema.schema}.{table}_provkey",),
    )
    assert (await cursor.fetchone())[0] == 1

    await _assert_resolves(provenance_service, semirings, test_schema)
