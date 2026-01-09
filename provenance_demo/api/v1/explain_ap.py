from logging import getLogger

from fastapi import Depends, HTTPException, Request

from provenance_demo.di import get_provenance_service

# TODO Reexport ProvenanceService in di.py?
from provenance_demo.services.provenance import ProvenanceService

logger = getLogger(__name__)


async def explain_ap(rq: Request, prov_svc: ProvenanceService = Depends(get_provenance_service)):
    # TODO: Parse AP
    try:
        fixture_rq = "SELECT distinct t.name, whyPROV_now(provenance(),'why_mapping') FROM assessment a JOIN platform__sna__questions q ON(a.question_id=q.id) JOIN platform__topic t ON(t.id=q.topic) WHERE id_lect=78 AND answer=-1 AND question_level=4;"
        prov = await prov_svc.compute_provenance(fixture_rq)

        if prov is None:
            raise HTTPException(status_code=404, detail="No data found")

        return {"explanation": prov}

    except Exception as e:
        logger.error("Failed to explain AP", exc_info=e)
        raise HTTPException(
            status_code=500, detail="Failed to compute provenance") from e
