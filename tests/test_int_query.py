from typing import List

import pytest

from ap_explanation.errors import (
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
    all_semirings: List[DbSemiring],
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
