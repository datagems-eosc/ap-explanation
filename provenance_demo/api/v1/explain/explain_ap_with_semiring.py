from json import loads
from typing import List

from fastapi import Depends, HTTPException, status

from provenance_demo.api.v1.dependencies.ap_parser import (
    ConnectionString,
    SchemaName,
    SqlOperator,
)
from provenance_demo.di import get_provenance_service_for_ap, get_semirings
from provenance_demo.errors import (
    SemiringOperationNotSupportedError,
    TableNotAnnotatedError,
)
from provenance_demo.types.semiring import DbSemiring


async def explain_ap_with_semiring(
    semiring_name: str,
    connection_string: ConnectionString,
    sql_node: SqlOperator,
    schema_name: SchemaName,
    all_semirings: List[DbSemiring] = Depends(get_semirings)
):
    """Explain the AP with only the chosen semiring using dynamic database connection."""

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

    # Create the service with the connection string from the AP
    service_factory = get_provenance_service_for_ap(connection_string)

    result = []
    # Use the factory to get the service
    async for service in service_factory():
        try:
            query = sql_node.properties["query"] if sql_node.properties else ""
            prov = await service.compute_provenance(schema_name, query, [semiring])
            result = loads(prov or "[]")
        except TableNotAnnotatedError as e:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        except SemiringOperationNotSupportedError as e:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        break  # Only process with first connection from pool

    return result
