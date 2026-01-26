
from typing import List

import pytest

from ap_explanation.errors import TableOrSchemaNotFoundError
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.semiring import DbSemiring
from tests.conftest import TestSchema


############################
# CREATE ANNOTATION TESTS #
############################
@pytest.mark.asyncio
async def test_ok_single_semiring(provenance_service: ProvenanceService, why_semiring: DbSemiring, test_schema: TestSchema):
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [why_semiring])


@pytest.mark.asyncio
async def test_ok_all_semiring(provenance_service: ProvenanceService, all_semirings: List[DbSemiring], test_schema: TestSchema):
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, all_semirings)


@pytest.mark.asyncio
async def test_ok_multiple_calls(provenance_service: ProvenanceService, all_semirings: List[DbSemiring], test_schema: TestSchema):
    """
    Annotate the same dataset multiple times with different semirings. This should work without issues.
    """
    for semiring in all_semirings:
        await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [semiring])


@pytest.mark.asyncio
async def test_ok_idempotency(provenance_service: ProvenanceService, why_semiring: DbSemiring, test_schema: TestSchema):
    """
    Annotating the same dataset multiple times with the same semiring should not be a problem.
    """
    newly_annotated = await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [why_semiring])
    assert newly_annotated is True

    newly_annotated = await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [why_semiring])
    assert newly_annotated is False


@pytest.mark.asyncio
async def test_ko_table_does_not_exists(provenance_service: ProvenanceService, why_semiring: DbSemiring, test_schema: TestSchema):
    with pytest.raises(TableOrSchemaNotFoundError) as exc_info:
        await provenance_service.annotate_dataset("i_dont_exists", test_schema.schema, [why_semiring])
    assert "i_dont_exists" in str(exc_info.value)
    assert test_schema.schema in str(exc_info.value)


@pytest.mark.asyncio
async def test_ko_schema_does_not_exists(provenance_service: ProvenanceService, why_semiring: DbSemiring, test_schema: TestSchema):
    with pytest.raises(TableOrSchemaNotFoundError) as exc_info:
        await provenance_service.annotate_dataset(test_schema.table, "i_dont_exists", [why_semiring])
    assert test_schema.table in str(exc_info.value)

######################
# REMOVE ANNOTATION TESTS #
######################


@pytest.mark.asyncio
async def test_ok_remove_annotation_from_non_annotated_table(provenance_service: ProvenanceService, why_semiring: DbSemiring, test_schema: TestSchema):
    was_removed = await provenance_service.remove_annotation(test_schema.table, test_schema.schema, [why_semiring])
    assert was_removed is False


@pytest.mark.asyncio
async def test_ok_reversibility(provenance_service: ProvenanceService, why_semiring: DbSemiring, test_schema: TestSchema):
    """
    Annotation should be reversible: annotating and then removing the annotation should leave the dataset unchanged.
    """
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema, [why_semiring])
    await provenance_service.remove_annotation(test_schema.table, test_schema.schema, [why_semiring])
