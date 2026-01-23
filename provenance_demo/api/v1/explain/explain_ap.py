from json import loads
from typing import List

from fastapi import Depends

from provenance_demo.api.v1.dependencies.ap_parser import SchemaName, SqlOperator
from provenance_demo.di import get_provenance_service, get_semirings
from provenance_demo.services.provenance import ProvenanceService
from provenance_demo.types.semiring import DbSemiring


async def explain_ap(
    sql_node: SqlOperator,
    schema_name: SchemaName,
    prov_svc: ProvenanceService = Depends(get_provenance_service),
    semirings: List[DbSemiring] = Depends(get_semirings)
):
    """Explain the AP with all available semirings."""
    query = sql_node.properties["query"] if sql_node.properties else ""
    prov = await prov_svc.compute_provenance(schema_name, query, semirings)

    return loads(prov or "[]")
