
import pytest

from provenance_demo.services.provenance import ProvenanceService
from provenance_demo.types.semiring import DbSemiring


@pytest.mark.asyncio
async def test_annotations(provenance_service: ProvenanceService, why_semiring: DbSemiring):
    await provenance_service.annotate_dataset("assessment", "mathe", [why_semiring])
