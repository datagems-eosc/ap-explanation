"""
Sharing one mapping table between all semirings does not change any result.

Why it cannot: a semiring call ``sr_x(token, 'x_mapping')`` only reads its
mapping view as a set of (value, provenance) pairs, and the per-semiring
tables earlier versions created are made by the same
``create_provenance_mapping`` call as the shared one (only the name differs),
then maintained alike by provenance_guard. Same pairs, same results.

What this test checks, in one database (row references are random UUIDs, so
two databases cannot be compared): the per-semiring tables hold the same pairs,
including a row inserted after annotation, and every query gives identical
answers, expressions and resolved rows before and after the migration to the
shared table.
"""

import orjson
import pytest
from psycopg import AsyncConnection

from ap_explanation.repository.annotation import AnnotationRepository
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.semiring import DbSemiring

SCHEMA = "mathe"
TABLES = ["assessment", "last_assessment_levels"]

QUERIES = [
    "SELECT * FROM assessment WHERE id < 20 OR id = 10000000",
    "SELECT a.id, l.last_level FROM assessment a"
    " JOIN last_assessment_levels l ON a.student_id = l.student_id AND a.topic = l.topic"
    " WHERE a.id < 50",
    "SELECT DISTINCT topic FROM assessment WHERE id < 500",
    "SELECT topic, COUNT(*) AS cnt FROM assessment WHERE id < 500 GROUP BY topic",
    "SELECT student_id FROM assessment WHERE id < 50"
    " UNION SELECT student_id FROM last_assessment_levels WHERE student_id < 60",
]


async def _results(service: ProvenanceService, semirings: list[DbSemiring]) -> dict[str, list]:
    """Every query's derivations, in a canonical order."""
    results = {}
    for query in QUERIES:
        rows = await service.compute_provenance(SCHEMA, query, semirings)
        dumped = [orjson.dumps(r.model_dump(mode="json"), option=orjson.OPT_SORT_KEYS) for r in rows]
        results[query] = sorted(dumped)
    return results


async def _insert_assessment(conn: AsyncConnection) -> None:
    """Clone a row under a fresh id: a genuine insert, which provenance_guard maps."""
    columns = (
        "student_id", "question_id", "topic", "subtopic", "question_level",
        "answer", "date", "duration", "option_selected",
    )
    async with conn.transaction():
        await conn.execute("SET LOCAL provsql.active = 0")
        cursor = await conn.execute(
            f"SELECT {', '.join(columns)} FROM {SCHEMA}.assessment ORDER BY id LIMIT 1"
        )
        values = list(await cursor.fetchone())
    await conn.execute(
        f"INSERT INTO {SCHEMA}.assessment (id, {', '.join(columns)})"
        f" VALUES (%s, {', '.join(['%s'] * len(columns))})",
        [10_000_000, *values],
    )


@pytest.mark.asyncio
async def test_shared_mapping_gives_the_same_results(
    provenance_service: ProvenanceService,
    annotation_repository: AnnotationRepository,
    db_connection: AsyncConnection,
    all_semirings: list[DbSemiring],
):
    for table in TABLES:
        await provenance_service.annotate_dataset(table, SCHEMA, all_semirings)

    # Back to the layout of earlier versions: one maintained mapping table
    # per semiring, made by the call they used, and views over them.
    await db_connection.execute(f"SET search_path TO {SCHEMA}, public, provsql")
    for table in TABLES:
        first, *others = all_semirings
        await db_connection.execute(
            f"ALTER TABLE {table}_provkey RENAME TO {first.get_legacy_provenance_table_name_for(table)}"
        )
        for semiring in others:
            await db_connection.execute(
                "SELECT create_provenance_mapping(%s, %s, 'provsql_ref', false, true)",
                (semiring.get_legacy_provenance_table_name_for(table), table),
            )
    for semiring in all_semirings:
        await annotation_repository._rebuild_union_mapping(SCHEMA, semiring)

    # Maintained alike: the guard appends the new row to each of them
    await _insert_assessment(db_connection)

    # The per-semiring tables hold the same pairs, so any one of them can
    # stand for all
    for table in TABLES:
        reference = all_semirings[0].get_legacy_provenance_table_name_for(table)
        for semiring in all_semirings[1:]:
            other = semiring.get_legacy_provenance_table_name_for(table)
            cursor = await db_connection.execute(
                f"SELECT (SELECT count(*) FROM (TABLE {reference} EXCEPT TABLE {other}) d),"
                f" (SELECT count(*) FROM (TABLE {other} EXCEPT TABLE {reference}) d),"
                f" (SELECT count(*) FROM {other} WHERE value LIKE 'assessment@k%%'"
                f"  AND provenance = (SELECT provsql FROM assessment WHERE id = 10000000))"
            )
            missing, extra, inserted = await cursor.fetchone()
            assert (missing, extra) == (0, 0), (reference, other)
            if table == "assessment":
                assert inserted == 1, other

    before = await _results(provenance_service, all_semirings)
    assert all(before.values()), "every query returns rows"

    # Annotating again migrates to the shared table
    for table in TABLES:
        await provenance_service.annotate_dataset(table, SCHEMA, all_semirings)
    cursor = await db_connection.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s AND tablename LIKE '%%\\_prov%%'",
        (SCHEMA,),
    )
    assert {row[0] for row in await cursor.fetchall()} == {f"{t}_provkey" for t in TABLES}

    after = await _results(provenance_service, all_semirings)
    for query in QUERIES:
        assert after[query] == before[query], query
