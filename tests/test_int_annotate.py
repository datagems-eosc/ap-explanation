

import pytest

from ap_explanation.domains.provenance.semiring import DbSemiring
from ap_explanation.errors import TableOrSchemaNotFoundError
from ap_explanation.services.provenance import ProvenanceService
from tests.conftest import TestSchema


############################
# CREATE ANNOTATION TESTS #
############################
@pytest.mark.asyncio
async def test_ok_idempotency(provenance_service: ProvenanceService, test_schema: TestSchema):
    """
    Annotating the same dataset multiple times should not be a problem.
    """
    newly_annotated = await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)
    assert newly_annotated is True

    newly_annotated = await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)
    assert newly_annotated is False


@pytest.mark.asyncio
async def test_ko_table_does_not_exists(provenance_service: ProvenanceService, test_schema: TestSchema):
    with pytest.raises(TableOrSchemaNotFoundError) as exc_info:
        await provenance_service.annotate_dataset("i_dont_exists", test_schema.schema)
    assert "i_dont_exists" in str(exc_info.value)
    assert test_schema.schema in str(exc_info.value)


@pytest.mark.asyncio
async def test_ko_schema_does_not_exists(provenance_service: ProvenanceService, test_schema: TestSchema):
    with pytest.raises(TableOrSchemaNotFoundError) as exc_info:
        await provenance_service.annotate_dataset(test_schema.table, "i_dont_exists")
    assert test_schema.table in str(exc_info.value)

######################
# REMOVE ANNOTATION TESTS #
######################


@pytest.mark.asyncio
async def test_ok_remove_annotation_from_non_annotated_table(provenance_service: ProvenanceService, test_schema: TestSchema):
    await provenance_service.remove_annotation(test_schema.table, test_schema.schema)


@pytest.mark.asyncio
async def test_ok_reversibility(provenance_service: ProvenanceService, test_schema: TestSchema):
    """
    Annotation should be reversible: annotating and then removing the annotation should leave the dataset unchanged.
    """
    # Cycle 1 - Annotate and remove
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)
    await provenance_service.remove_annotation(test_schema.table, test_schema.schema)

    # Cycle 2 - Annotate and remove again to check that the first cycle did not cause any issues
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)
    await provenance_service.remove_annotation(test_schema.table, test_schema.schema)


@pytest.mark.asyncio
async def test_ok_removing_one_table_keeps_the_others(
    provenance_service: ProvenanceService, why_semiring: DbSemiring, test_schema: TestSchema
):
    """The tables of a schema share one mapping view, which must not need the table removed."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)
    await provenance_service.annotate_dataset("last_assessment_levels", test_schema.schema)

    await provenance_service.remove_annotation("last_assessment_levels", test_schema.schema)

    rows = await provenance_service.compute_provenance(
        test_schema.schema, f"SELECT * FROM {test_schema.schema}.{test_schema.table} LIMIT 3", [why_semiring]
    )
    assert rows
    assert all(row.provenance[why_semiring.name].data for row in rows)
