from json import loads
from typing import List

from fastapi import Depends

from provenance_demo.api.v1.dependencies.ap_parser import (
    ConnectionString,
    SchemaName,
    SqlOperator,
)
from provenance_demo.di import get_provenance_service_for_ap, get_semirings
from provenance_demo.types.semiring import DbSemiring


async def explain_ap(
    connection_string: ConnectionString,
    sql_node: SqlOperator,
    schema_name: SchemaName,
    semirings: List[DbSemiring] = Depends(get_semirings)
):
    """Explain the AP with all available semirings using dynamic database connection."""
    # Create the service with the connection string from the AP
    service_factory = get_provenance_service_for_ap(connection_string)

    result = []
    # Use the factory to get the service
    async for service in service_factory():
        query = sql_node.properties["query"] if sql_node.properties else ""
        prov = await service.compute_provenance(schema_name, query, semirings)
        result = loads(prov or "[]")
        break  # Only process with first connection from pool

    return result
