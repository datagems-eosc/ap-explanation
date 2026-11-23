
import pytest

from ap_explanation.domains.provenance.semiring import DbSemiring
from ap_explanation.errors import AggregateSemiringError
from ap_explanation.services.provenance import ProvenanceService
from tests.conftest import TestSchema


@pytest.mark.asyncio
async def test_ok_compute_provenance_with_all_semirings(
    provenance_service: ProvenanceService,
    all_semirings: list[DbSemiring],
    test_schema: TestSchema
):
    """Each result row has its provenance in every semiring: an expression and the tuples it names."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 3"
    results = await provenance_service.compute_provenance(test_schema.schema, query, all_semirings)

    assert results
    for row in results:
        assert row.answer
        assert set(row.provenance) == {s.name for s in all_semirings}
        for provenance in row.provenance.values():
            assert provenance.expression is not None
            assert isinstance(provenance.data, list)


@pytest.mark.asyncio
@pytest.mark.parametrize("projection, expected_rows", [("student_id", 1), ("*", 2)])
async def test_ok_compute_provenance_distinct_merges_rows(
    provenance_service: ProvenanceService,
    all_semirings: list[DbSemiring],
    test_schema: TestSchema,
    two_rows_of_one_student: str,
    projection: str,
    expected_rows: int,
):
    """
    A DISTINCT query returns one derivation per result row, whose provenance
    combines every tuple merged into it, not one derivation per tuple.
    """
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    query = f"SELECT DISTINCT {projection} FROM {test_schema.table} WHERE id IN ({two_rows_of_one_student})"
    results = await provenance_service.compute_provenance(test_schema.schema, query, all_semirings)

    assert len(results) == expected_rows
    for row in results:
        assert set(row.provenance.keys()) == {s.name for s in all_semirings}
        if expected_rows == 1:
            # Both tuples are referenced by the merged row
            assert len(row.provenance["why"].data) == 2


@pytest.mark.asyncio
async def test_ok_compute_provenance_union_merges_rows(
    provenance_service: ProvenanceService,
    all_semirings: list[DbSemiring],
    test_schema: TestSchema,
    two_rows_of_one_student: str,
):
    """A row found in both branches of a UNION comes back once, with both tuples."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    first, second = two_rows_of_one_student.split(", ")
    query = (
        f"SELECT student_id FROM {test_schema.table} WHERE id = {first}"
        f" UNION SELECT student_id FROM {test_schema.table} WHERE id = {second}"
    )
    results = await provenance_service.compute_provenance(test_schema.schema, query, all_semirings)

    assert len(results) == 1
    assert "student_id" in results[0].answer
    assert set(results[0].provenance) == {s.name for s in all_semirings}
    assert len(results[0].provenance["why"].data) == 2


@pytest.mark.asyncio
async def test_ok_compute_provenance_aggregation_with_all_semirings(
    provenance_service: ProvenanceService,
    all_semirings: list[DbSemiring],
    test_schema: TestSchema,
    two_rows_of_one_student: str,
):
    """
    An aggregate is explained in the semirings that support it only (formula),
    rather than failing on the others ("does not support value gates").
    """
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    query = f"SELECT student_id, COUNT(*) AS cnt FROM {test_schema.table} WHERE id IN ({two_rows_of_one_student}) GROUP BY student_id"
    results = await provenance_service.compute_provenance(test_schema.schema, query, all_semirings)

    assert len(results) == 1
    assert set(results[0].provenance) == {"formula"}
    assert len(results[0].provenance["formula"].data) == 2
    # The aggregate's value stays in the answer
    assert "cnt" in results[0].answer


@pytest.mark.asyncio
async def test_ok_compute_provenance_aggregation_without_group_by(
    provenance_service: ProvenanceService,
    formula_semiring: DbSemiring,
    test_schema: TestSchema,
    two_rows_of_one_student: str,
):
    """Without GROUP BY too, the formula lists the tuples counted, rather than being empty."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    query = f"SELECT COUNT(*) FROM {test_schema.table} WHERE id IN ({two_rows_of_one_student})"
    results = await provenance_service.compute_provenance(test_schema.schema, query, [formula_semiring])

    assert len(results) == 1
    assert len(results[0].provenance["formula"].data) == 2


@pytest.mark.asyncio
async def test_ok_compute_provenance_aggregation_next_to_unsupported_one(
    provenance_service: ProvenanceService,
    formula_semiring: DbSemiring,
    test_schema: TestSchema,
    two_rows_of_one_student: str,
):
    """COUNT's formula lists the tuples stddev read too; stddev keeps its value in the answer."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    query = (
        f"SELECT stddev(question_level) AS sd, COUNT(*) AS n"
        f" FROM {test_schema.table} WHERE id IN ({two_rows_of_one_student})"
    )
    results = await provenance_service.compute_provenance(test_schema.schema, query, [formula_semiring])

    assert len(results) == 1
    assert "sd" in results[0].answer
    assert len(results[0].provenance["formula"].data) == 2


@pytest.mark.asyncio
async def test_ko_compute_provenance_aggregation_without_aggregate_semiring(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
):
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    query = f"SELECT topic, COUNT(*) FROM {test_schema.table} GROUP BY topic"
    with pytest.raises(AggregateSemiringError):
        await provenance_service.compute_provenance(test_schema.schema, query, [why_semiring])


@pytest.mark.asyncio
async def test_ok_compute_provenance_aggregation_over_no_row(
    provenance_service: ProvenanceService,
    formula_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """An aggregate over no row has no formula (NULL), hence no provenance."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    query = f"SELECT AVG(question_level) FROM {test_schema.table} WHERE id = -1"
    results = await provenance_service.compute_provenance(test_schema.schema, query, [formula_semiring])

    assert len(results) == 1
    assert results[0].provenance["formula"].data == []
