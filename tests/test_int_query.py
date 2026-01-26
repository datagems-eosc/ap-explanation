from typing import List

import orjson
import pytest

from provenance_demo.errors import (
    SemiringOperationNotSupportedError,
    TableNotAnnotatedError,
)
from provenance_demo.services.provenance import ProvenanceService
from provenance_demo.types.semiring import DbSemiring
from tests.conftest import TestSchema, formula_semiring


@pytest.mark.asyncio
async def test_ok_compute_provenance_with_single_semiring(
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

    # Verify we got valid JSON with results
    assert result_json is not None
    results = orjson.loads(result_json)
    assert len(results) == 1  # One result per semiring
    assert len(results[0]) > 0  # Has rows


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

    # Verify we got results for all semirings
    assert result_json is not None
    results = orjson.loads(result_json)
    assert len(results) == len(all_semirings)


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
    results = orjson.loads(result_json)
    assert len(results) == 1
    assert len(results[0]) > 0


@pytest.mark.asyncio
async def test_ko_aggregation_not_supported(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema
):
    """Test that aggregation queries with non-aggregation semirings raise SemiringOperationNotSupportedError."""
    # First annotate the table
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [why_semiring])

    # Aggregation query with a semiring that doesn't support aggregation
    query = f"SELECT topic, COUNT(*) as cnt FROM {test_schema.schema}.{test_schema.table} GROUP BY topic LIMIT 5"

    with pytest.raises(SemiringOperationNotSupportedError) as exc_info:
        await provenance_service.compute_provenance(test_schema.schema, query, [why_semiring])

    assert why_semiring.name in str(exc_info.value)
    assert "aggregate" in str(exc_info.value).lower()
