from json import loads
from typing import List

from fastapi import Depends, HTTPException, status

from provenance_demo.api.v1.dependencies.ap_parser import SchemaName, SqlOperator
from provenance_demo.di import get_provenance_service, get_semirings
from provenance_demo.services.provenance import ProvenanceService
from provenance_demo.types.semiring import DbSemiring


async def explain_ap_with_semiring(
    semiring_name: str,
    sql_node: SqlOperator,
    schema_name: SchemaName,
    prov_svc: ProvenanceService = Depends(get_provenance_service),
    all_semirings: List[DbSemiring] = Depends(get_semirings)
):
    """Explain the AP with only the chosen semiring."""

    # Find the requested semiring
    semiring = next(
        (s for s in all_semirings if s.name == semiring_name), None
    )
    if not semiring:
        available = ", ".join([s.name for s in all_semirings])
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"Semiring '{semiring_name}' not found. Available semirings: {available}"
        )

    query = sql_node.properties["query"] if sql_node.properties else ""
    prov = await prov_svc.compute_provenance(schema_name, query, [semiring])

    return loads(prov or "[]")
