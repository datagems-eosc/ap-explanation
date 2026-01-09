from json import loads
from logging import getLogger

from fastapi import Depends, HTTPException, status

from provenance_demo.di import get_provenance_service

# TODO Reexport ProvenanceService in di.py?
from provenance_demo.services.provenance import ProvenanceService
from provenance_demo.types.pg_json import PgJson

logger = getLogger(__name__)


async def explain_ap(ap: PgJson, prov_svc: ProvenanceService = Depends(get_provenance_service)):
    # TODO: Parse AP
    try:
        sql_nodes = ap.get_nodes_by_label("SQL_Operator")
        match len(sql_nodes):
            case 0:
                raise HTTPException(
                    status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail="This AP has no SQL Operators !"
                )
            case n if n > 1:
                logger.warning(
                    "Multiples SQL_OPERATOR nodes detected, only the first one will be processed")

        # Check that the found SQL node is well formed
        sql_node = sql_nodes[0]
        if not sql_node.properties or "Query" not in sql_node.properties.keys():
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="Malformed AP : SQL operator has no Query property"
            )

        prov = await prov_svc.compute_provenance(sql_node.properties["Query"])

        return loads(prov or "[]")

    except Exception as e:
        logger.error("Failed to explain AP", exc_info=e)
        raise HTTPException(
            status_code=500, detail="Failed to compute provenance") from e
