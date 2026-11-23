

import pytest

from ap_explanation.domains.provenance.semiring import DbSemiring
from ap_explanation.errors import TableOrSchemaNotFoundError
from ap_explanation.services.provenance import ProvenanceService
from tests.conftest import TestSchema


@pytest.mark.asyncio
async def test_ok_annotation_lifecycle(provenance_service: ProvenanceService, test_schema: TestSchema):
    """Annotating and removing are idempotent, and a removed annotation is really gone."""
    async def annotate() -> bool:
        return await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    async def remove() -> None:
        await provenance_service.remove_annotation(test_schema.table, test_schema.schema)

    assert await annotate() is True
    assert await annotate() is False  # already annotated
    await remove()
    await remove()  # no longer annotated: nothing to do
    assert await annotate() is True  # annotated anew


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
