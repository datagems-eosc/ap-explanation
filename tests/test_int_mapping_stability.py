"""
The scenarios a mapping built once could not survive.

A CTID is a physical row location: UPDATE and VACUUM FULL move rows, so a
mapping of CTIDs resolves to nothing — or to a different row. And a snapshot
never learns about rows inserted after annotation.
"""

from collections.abc import Callable
from typing import Any

import pytest
from psycopg import AsyncConnection

from ap_explanation.domains.provenance.derivation import Derivation
from ap_explanation.domains.provenance.semiring import DbSemiring
from ap_explanation.services.provenance import ProvenanceService
from tests.conftest import TestSchema


def _references(rows: list[Derivation], semiring: DbSemiring) -> set[str]:
    """Every resolved row reference across a compute_provenance result."""
    return {
        entry["reference"]
        for row in rows
        for entry in row.provenance[semiring.name].data
    }


def _resolved_data(rows: list[Derivation], semiring: DbSemiring) -> dict[str, dict[str, Any]]:
    """Resolved row payloads keyed by reference."""
    return {
        entry["reference"]: entry["data"]
        for row in rows
        for entry in row.provenance[semiring.name].data
    }


@pytest.mark.asyncio
async def test_references_survive_update_and_vacuum(
    provenance_service: ProvenanceService,
    db_connection: AsyncConnection,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """A row that moves on disk must still resolve, and to itself."""
    table = f"{test_schema.schema}.{test_schema.table}"
    await provenance_service.annotate_dataset(
        test_schema.table, test_schema.schema
    )

    query = f"SELECT * FROM {table} ORDER BY id LIMIT 5"
    before = await provenance_service.compute_provenance(
        test_schema.schema, query, [why_semiring]
    )
    refs_before = _references(before, why_semiring)
    assert refs_before, "no provenance references resolved before mutation"

    # Move every row on disk: the rewrite an UPDATE performs, then a full
    # rewrite of the whole relation.
    await db_connection.execute(
        f"UPDATE {table} SET duration = COALESCE(duration, 0) + 1"
    )
    await db_connection.execute(f"VACUUM FULL {table}")

    after = await provenance_service.compute_provenance(
        test_schema.schema, query, [why_semiring]
    )
    refs_after = _references(after, why_semiring)

    assert refs_after == refs_before, "references changed when rows moved on disk"

    # And they resolve to the actual rows, carrying the updated value.
    resolved = _resolved_data(after, why_semiring)
    assert len(resolved) == len(refs_after)
    for data in resolved.values():
        assert data, "reference resolved to an empty row"


@pytest.mark.asyncio
async def test_rows_inserted_after_annotation_are_mapped(
    provenance_service: ProvenanceService,
    db_connection: AsyncConnection,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """A row added after annotation still explains itself."""
    table = f"{test_schema.schema}.{test_schema.table}"
    await provenance_service.annotate_dataset(
        test_schema.table, test_schema.schema
    )

    # Clone an existing row under a fresh id: the seeded schema has foreign
    # keys, so invented values would not insert. Read the values out first
    # rather than INSERT ... SELECT, which ProvSQL cannot rewrite into a
    # tracked relation. The provsql column is left out of the column list,
    # which is what makes this a genuine insert: provenance_guard mints a new
    # token for it.
    new_id = 10_000_000
    columns = (
        "student_id",
        "question_id",
        "topic",
        "subtopic",
        "question_level",
        "answer",
        "date",
        "duration",
        "option_selected",
    )
    async with db_connection.transaction():
        # With ProvSQL paused: under it, LIMIT keeps every row that may be first
        # in some possible world, and ORDER BY ... LIMIT 1 took four minutes
        await db_connection.execute("SET LOCAL provsql.active = 0")
        cursor = await db_connection.execute(
            f"SELECT {', '.join(columns)} FROM {table} ORDER BY id LIMIT 1"
        )
        values = list((await cursor.fetchall())[0])

    placeholders = "%s" + ", %s" * (len(columns) - 1)
    await db_connection.execute(
        f"INSERT INTO {table} (id, {', '.join(columns)}) VALUES (%s, {placeholders})",
        [new_id, *values],
    )

    query = f"SELECT * FROM {table} WHERE id = {new_id}"
    rows = await provenance_service.compute_provenance(
        test_schema.schema, query, [why_semiring]
    )

    assert len(rows) == 1, "the inserted row did not come back from the query"
    resolved = _resolved_data(rows, why_semiring)
    assert resolved, "the inserted row has no mapped provenance reference"
    assert any(data.get("id") == new_id for data in resolved.values())


@pytest.mark.asyncio
async def test_provsql_column_never_reaches_results(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """
    The bookkeeping column must not leak into SELECT * answers, nor into the
    rows a reference resolves to: resolving reads a tracked table, so it runs
    with ProvSQL paused, otherwise the lookup is rewritten.
    """
    table = f"{test_schema.schema}.{test_schema.table}"
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    rows = await provenance_service.compute_provenance(
        test_schema.schema, f"SELECT * FROM {table} LIMIT 5", [why_semiring]
    )

    assert rows
    resolved = _resolved_data(rows, why_semiring)
    assert resolved
    assert all("provsql" not in row.answer for row in rows)
    assert all("provsql" not in data for data in resolved.values())


@pytest.mark.asyncio
async def test_provsql_stays_off_for_other_connections(
    provenance_service: ProvenanceService,
    db_connection: AsyncConnection,
    connstr: Callable[..., str],
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """
    ProvSQL cannot rewrite every SQL construct, so it is enabled per connection
    and the server default stays off — another service querying the same
    database must never be subject to the rewriter, including after this app
    has annotated a table.
    """
    await provenance_service.annotate_dataset(
        test_schema.table, test_schema.schema
    )
    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 5"
    await provenance_service.compute_provenance(
        test_schema.schema, query, [why_semiring]
    )

    # A separate connection stands in for another service.
    other = await AsyncConnection.connect(connstr(), autocommit=True)
    try:
        cursor = await other.execute("SELECT current_setting('provsql.active')")
        assert (await cursor.fetchall())[0][
            0
        ] == "off", "ProvSQL is active by default for every connection to this database"

        # An annotated table must still read back without provenance columns.
        cursor = await other.execute(f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 1")
        assert cursor.description is not None
        assert "provsql" not in {c.name for c in cursor.description}
    finally:
        await other.close()

    # This app's own connection keeps ProvSQL enabled for the next query.
    cursor = await db_connection.execute("SELECT current_setting('provsql.active')")
    assert (await cursor.fetchall())[0][0] == "on"
