"""
The scenarios the previous CTID-based mapping could not survive.

A CTID is a physical row location: UPDATE and VACUUM FULL move rows, so a
mapping built from CTIDs resolves to nothing — or to a different row. And a
snapshot mapping never learns about rows inserted after annotation.
"""

import pytest
from psycopg import AsyncConnection

from ap_explanation.repository.mapping.key_mapping import REFERENCE_COLUMN
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.semiring import DbSemiring
from tests.conftest import TestSchema


def table_of(test_schema: TestSchema) -> str:
    return f"{test_schema.schema}.{test_schema.table}"


def _references(rows, semiring: DbSemiring) -> set[str]:
    """Every resolved row reference across a compute_provenance result."""
    return {
        entry["reference"]
        for row in rows
        for entry in row.provenance[semiring.name].data
    }


def _resolved_data(rows, semiring: DbSemiring) -> dict[str, dict]:
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
        test_schema.table, test_schema.schema, [why_semiring]
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
    """
    ProvSQL's maintained mappings (>= 1.11.0) append a mapping row for every
    genuine INSERT, so a row added after annotation still explains itself.
    """
    table = f"{test_schema.schema}.{test_schema.table}"
    await provenance_service.annotate_dataset(
        test_schema.table, test_schema.schema, [why_semiring]
    )

    # Clone an existing row under a fresh id: the seeded schema has foreign
    # keys, so invented values would not insert. Read the values out first
    # rather than INSERT ... SELECT, which ProvSQL cannot rewrite into a
    # tracked relation. The provsql column is left out of the column list,
    # which is what makes this a genuine insert — provenance_guard mints a new
    # token for it, and with a maintained mapping a mapping row to go with it.
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
    cursor = await db_connection.execute(
        f"SELECT {', '.join(columns)} FROM {table} ORDER BY id LIMIT 1"
    )
    values = list((await cursor.fetchone())[: len(columns)])

    placeholders = ", ".join(["%s"] * len(columns))
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
async def test_reference_column_is_hidden_from_answers(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """The bookkeeping column must not leak into SELECT * results."""
    table = f"{test_schema.schema}.{test_schema.table}"
    await provenance_service.annotate_dataset(
        test_schema.table, test_schema.schema, [why_semiring]
    )

    query = f"SELECT * FROM {table} LIMIT 5"
    rows = await provenance_service.compute_provenance(
        test_schema.schema, query, [why_semiring]
    )

    assert rows
    for row in rows:
        assert REFERENCE_COLUMN not in row.answer
        for entry in row.provenance[why_semiring.name].data:
            assert REFERENCE_COLUMN not in entry["data"]


@pytest.mark.asyncio
async def test_provsql_stays_off_for_other_connections(
    provenance_service: ProvenanceService,
    db_connection: AsyncConnection,
    connstr,
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
        test_schema.table, test_schema.schema, [why_semiring]
    )
    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 5"
    await provenance_service.compute_provenance(
        test_schema.schema, query, [why_semiring]
    )

    # A separate connection stands in for another service.
    other = await AsyncConnection.connect(connstr(), autocommit=True)
    try:
        cursor = await other.execute("SELECT current_setting('provsql.active')")
        assert (await cursor.fetchone())[
            0
        ] == "off", "ProvSQL is active by default for every connection to this database"

        # An annotated table must still read back without provenance columns.
        cursor = await other.execute(f"SELECT * FROM {table_of(test_schema)} LIMIT 1")
        assert cursor.description is not None
        assert "provsql" not in {c.name for c in cursor.description}
    finally:
        await other.close()

    # This app's own connection keeps ProvSQL enabled for the next query.
    cursor = await db_connection.execute("SELECT current_setting('provsql.active')")
    assert (await cursor.fetchone())[0] == "on"


@pytest.mark.asyncio
async def test_reference_resolution_is_not_rewritten(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """
    Resolving a reference reads a tracked table, so it is run with ProvSQL
    paused — otherwise the lookup is rewritten and a provsql column lands in
    the payload handed to callers.
    """
    table = f"{test_schema.schema}.{test_schema.table}"
    await provenance_service.annotate_dataset(
        test_schema.table, test_schema.schema, [why_semiring]
    )

    query = f"SELECT * FROM {table} LIMIT 5"
    rows = await provenance_service.compute_provenance(
        test_schema.schema, query, [why_semiring]
    )

    assert rows
    resolved = _resolved_data(rows, why_semiring)
    assert resolved
    for data in resolved.values():
        assert "provsql" not in data


@pytest.mark.asyncio
async def test_outdated_ctid_mapping_is_rebuilt(
    provenance_service: ProvenanceService,
    db_connection: AsyncConnection,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """
    A database annotated by an earlier version holds a mapping of CTIDs, with
    no reference column and no registry entry. Re-annotating must rebuild it
    rather than skip over it.
    """
    table = f"{test_schema.schema}.{test_schema.table}"
    prov_table = why_semiring.get_provenance_table_name_for(test_schema.table)
    qualified = f"{test_schema.schema}.{prov_table}"

    await provenance_service.annotate_dataset(
        test_schema.table, test_schema.schema, [why_semiring]
    )

    # Wind the mapping back to what the previous version left behind: CTID
    # values, no maintained registration, and no reference column.
    await db_connection.execute(
        f"UPDATE {qualified} SET value = '{test_schema.table}@p0r1'"
    )
    await db_connection.execute(
        "DELETE FROM provsql.provenance_mapping_registry WHERE mapping = to_regclass(%s)::oid",
        (qualified,),
    )
    await db_connection.execute(f"ALTER TABLE {table} DROP COLUMN {REFERENCE_COLUMN}")

    await provenance_service.annotate_dataset(
        test_schema.table, test_schema.schema, [why_semiring]
    )

    cursor = await db_connection.execute(f"SELECT value FROM {qualified} LIMIT 1")
    value = (await cursor.fetchone())[0]
    why_semiring.mappingStrategy.decode(value)  # raises ValueError if still old format

    # And the rebuilt mapping still resolves.
    query = f"SELECT * FROM {table} LIMIT 5"
    rows = await provenance_service.compute_provenance(
        test_schema.schema, query, [why_semiring]
    )
    assert _references(rows, why_semiring)
