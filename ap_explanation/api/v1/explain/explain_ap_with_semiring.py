from json import loads
from typing import List

from fastapi import Depends, HTTPException, status

from ap_explanation.api.v1.dependencies.ap_parser import (
    DatabaseName,
    SchemaName,
    SqlOperator,
)
from ap_explanation.di import get_provenance_service_for_ap, get_semirings
from ap_explanation.errors import (
    ProvSqlInternalError,
    ProvSqlMissingError,
    SemiringOperationNotSupportedError,
    TableNotAnnotatedError,
)
from ap_explanation.types.semiring import DbSemiring


async def explain_ap_with_semiring(
    semiring_name: str,
    db_name: DatabaseName,
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

    # Create the service with the database name from the AP
    service_factory = get_provenance_service_for_ap(db_name)

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
        except ProvSqlInternalError as e:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                detail=f"Provenance computation failed: {str(e)}"
            )
        except ProvSqlMissingError as e:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"ProvSQL extension is not installed or not available on the PostgreSQL server: {str(e)}"
            )
        break  # Only process with first connection from pool

    return result
