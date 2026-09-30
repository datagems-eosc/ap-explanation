
import pytest

from ap_explanation.errors import (
    AggregateSemiringError,
    TableNotAnnotatedError,
)
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.semiring import DbSemiring
from tests.conftest import TestSchema


@pytest.mark.asyncio
async def test_ok_compute_provenance_why_semiring(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema
):
    """Test computing provenance with a single semiring."""
    # First annotate the table
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [why_semiring])

    # Query with provenance
    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 5"
    result_json = await provenance_service.compute_provenance(test_schema.schema, query, [why_semiring])

    # Verify we got valid results
    assert result_json is not None
    results = result_json
    assert len(results) > 0  # Has rows

    # Verify each result has the new structure
    for row in results:
        assert row.answer
        assert row.provenance
        assert why_semiring.name in row.provenance
        assert row.provenance[why_semiring.name].expression is not None
        assert isinstance(row.provenance[why_semiring.name].data, list)


@pytest.mark.asyncio
async def test_ok_compute_provenance_formula_semiring(
    provenance_service: ProvenanceService,
    formula_semiring: DbSemiring,
    test_schema: TestSchema
):
    """Test computing provenance with a single semiring."""
    # First annotate the table
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [formula_semiring])

    # Query with provenance
    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 5"
    result_json = await provenance_service.compute_provenance(test_schema.schema, query, [formula_semiring])

    # Verify we got valid results
    assert result_json is not None
    results = result_json
    assert len(results) > 0  # Has rows

    # Verify each result has the new structure
    for row in results:
        assert row.answer
        assert row.provenance
        assert formula_semiring.name in row.provenance
        assert row.provenance[formula_semiring.name].expression is not None
        assert isinstance(row.provenance[formula_semiring.name].data, list)


@pytest.mark.asyncio
async def test_ok_compute_provenance_boolexpr_semiring(
    provenance_service: ProvenanceService,
    boolexpr_semiring: DbSemiring,
    test_schema: TestSchema
):
    """Test computing provenance with a single semiring."""
    # First annotate the table
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [boolexpr_semiring])

    # Query with provenance
    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 5"
    result_json = await provenance_service.compute_provenance(test_schema.schema, query, [boolexpr_semiring])

    # Verify we got valid results
    assert result_json is not None
    results = result_json
    assert len(results) > 0  # Has rows

    # Verify each result has the new structure
    for row in results:
        assert row.answer
        assert row.provenance
        assert boolexpr_semiring.name in row.provenance
        assert row.provenance[boolexpr_semiring.name].expression is not None
        assert isinstance(row.provenance[boolexpr_semiring.name].data, list)


@pytest.mark.asyncio
async def test_ok_compute_provenance_how_semiring(
    provenance_service: ProvenanceService,
    how_semiring: DbSemiring,
    test_schema: TestSchema
):
    """Test computing provenance with the how-provenance semiring."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [how_semiring])

    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 5"
    results = await provenance_service.compute_provenance(test_schema.schema, query, [how_semiring])

    assert len(results) > 0
    for row in results:
        assert row.answer
        assert how_semiring.name in row.provenance
        assert row.provenance[how_semiring.name].expression is not None
        assert isinstance(row.provenance[how_semiring.name].data, list)


@pytest.mark.asyncio
async def test_ok_compute_provenance_which_semiring(
    provenance_service: ProvenanceService,
    which_semiring: DbSemiring,
    test_schema: TestSchema
):
    """Test computing provenance with the which-provenance (lineage) semiring."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [which_semiring])

    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 5"
    results = await provenance_service.compute_provenance(test_schema.schema, query, [which_semiring])

    assert len(results) > 0
    for row in results:
        assert row.answer
        assert which_semiring.name in row.provenance
        assert row.provenance[which_semiring.name].expression is not None
        assert isinstance(row.provenance[which_semiring.name].data, list)


@pytest.mark.asyncio
async def test_ok_compute_provenance_with_all_semirings(
    provenance_service: ProvenanceService,
    all_semirings: list[DbSemiring],
    test_schema: TestSchema
):
    """Test computing provenance with all available semirings."""
    # First annotate the table
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, all_semirings)

    # Query with provenance
    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 3"
    result_json = await provenance_service.compute_provenance(test_schema.schema, query, all_semirings)

    # Verify we got results with all semirings merged
    assert result_json is not None
    results = result_json
    assert len(results) > 0

    # Each row should have provenance data for every semiring
    for row in results:
        assert row.answer
        assert row.provenance
        all_semiring_names = {s.name for s in all_semirings}
        assert set(row.provenance.keys()) == all_semiring_names


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
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, all_semirings)

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
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, all_semirings)

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
async def test_ok_compute_provenance_without_annotation(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema
):
    """Test that querying without annotation raises TableNotAnnotatedError."""
    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 5"

    with pytest.raises(TableNotAnnotatedError) as exc_info:
        await provenance_service.compute_provenance(test_schema.schema, query, [why_semiring])

    assert "not annotated" in str(exc_info.value).lower()
    assert why_semiring.name in str(exc_info.value)


@pytest.mark.asyncio
async def test_ok_compute_provenance_with_aggregation(
    provenance_service: ProvenanceService,
    formula_semiring: DbSemiring,
    test_schema: TestSchema
):
    """Test computing provenance with aggregation query."""
    # First annotate the table
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [formula_semiring])

    # Aggregation query
    query = f"SELECT topic, COUNT(*) as cnt FROM {test_schema.schema}.{test_schema.table} GROUP BY topic LIMIT 5"
    result_json = await provenance_service.compute_provenance(test_schema.schema, query, [formula_semiring])

    # Verify we got valid results
    assert result_json is not None
    results = result_json
    assert len(results) > 0

    for row in results:
        assert row.answer
        assert row.provenance
        assert formula_semiring.name in row.provenance


@pytest.mark.asyncio
async def test_ok_explain_why(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema
):
    """Test that explain returns a non-empty string for valid provenance results."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [why_semiring])

    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 3"
    provenance_results = await provenance_service.compute_provenance(test_schema.schema, query, [why_semiring])
    assert len(provenance_results) > 0

    explanation = await provenance_service.explain(test_schema.schema, query, provenance_results)

    assert explanation is not None
    assert isinstance(explanation, str)
    assert len(explanation) > 0


@pytest.mark.asyncio
async def test_ok_explain_formula(
    provenance_service: ProvenanceService,
    formula_semiring: DbSemiring,
    test_schema: TestSchema
):
    """Test that explain returns a non-empty string for valid provenance results."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [formula_semiring])

    query = f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 3"
    provenance_results = await provenance_service.compute_provenance(test_schema.schema, query, [formula_semiring])
    assert len(provenance_results) > 0

    explanation = await provenance_service.explain(test_schema.schema, query, provenance_results)

    assert explanation is not None
    assert isinstance(explanation, str)
    assert len(explanation) > 0


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
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, all_semirings)

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
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [formula_semiring])

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
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [formula_semiring])

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
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [why_semiring])

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
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [formula_semiring])

    query = f"SELECT AVG(question_level) FROM {test_schema.table} WHERE id = -1"
    results = await provenance_service.compute_provenance(test_schema.schema, query, [formula_semiring])

    assert len(results) == 1
    assert results[0].provenance["formula"].data == []
