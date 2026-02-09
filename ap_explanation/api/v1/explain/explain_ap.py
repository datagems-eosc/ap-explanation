from json import loads
from typing import List

from fastapi import Depends, HTTPException, status

from ap_explanation.api.v1.dependencies.ap_parser import (
    ConnectionString,
    SchemaName,
    SqlOperator,
    TableNames,
)
from ap_explanation.di import get_provenance_service_for_ap, get_semirings
from ap_explanation.errors import (
    ProvSqlMissingError,
    SemiringOperationNotSupportedError,
    TableNotAnnotatedError,
)
from ap_explanation.types.semiring import DbSemiring


async def explain_ap(
    connection_string: ConnectionString,
    sql_node: SqlOperator,
    schema_name: SchemaName,
    tables_names: TableNames,
    semirings: List[DbSemiring] = Depends(get_semirings)
):
    """Explain the AP with all available semirings using dynamic database connection."""
    # Create the service with the connection string from the AP
    service_factory = get_provenance_service_for_ap(connection_string)

    result = []
    # Use the factory to get the service
    async for service in service_factory():
        try:
            query = sql_node.properties["query"] if sql_node.properties else ""
            prov = await service.compute_provenance(schema_name, query, semirings)
            # NOTE : This is to mitigate https://github.com/PierreSenellart/provsql/issues/67
            # Leaving provenance enabled will prevent user from running some queries on the database,
            # so we need to remove the annotation after computing the provenance
            # This is a workaround and should be removed when possible, as it makes computing provenance
            # very expensive, but it is necessary to avoid blocking the database for other users
            for table_name in tables_names:
                await service.remove_annotation(table_name, schema_name)
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
        except ProvSqlMissingError as e:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"ProvSQL extension is not installed or not available on the PostgreSQL server: {str(e)}"
            )
        break  # Only process with first connection from pool

    return result
