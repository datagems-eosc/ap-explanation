from logging import getLogger
from typing import List, Literal

from fastapi import Depends, HTTPException, status
from pydantic import BaseModel

from provenance_demo.api.v1.dependencies.ap_parser import (
    ConnectionString,
    SchemaName,
    TableNames,
)
from provenance_demo.di import get_provenance_service_for_ap, get_semirings
from provenance_demo.errors import TableOrSchemaNotFoundError
from provenance_demo.types.semiring import DbSemiring

logger = getLogger(__name__)


class RemovalResult(BaseModel):
    table_name: str
    semiring: str
    status: Literal["success", "not_found", "error"]
    message: str


async def remove_annotation_ap(
    connection_string: ConnectionString,
    tables_names: TableNames,
    schema_name: SchemaName,
    semirings: List[DbSemiring] = Depends(get_semirings)
) -> List[RemovalResult]:
    """Remove annotations from AP tables for all available semirings using dynamic database connection."""
    logger.info(
        f"Removing annotations from tables: {tables_names} for all semirings")
    results: List[RemovalResult] = []

    # Create the service with the connection string from the AP
    service_factory = get_provenance_service_for_ap(connection_string)

    # Use the factory to get the service
    async for prov_svc in service_factory():
        for table_name in tables_names:
            try:
                was_removed = await prov_svc.remove_annotation(table_name, schema_name, semirings)
                for semiring in semirings:
                    if was_removed:
                        results.append(
                            RemovalResult(
                                table_name=table_name,
                                semiring=semiring.name,
                                status="success",
                                message=f"Annotations for table '{table_name}' with semiring '{semiring.name}' were successfully removed"
                            )
                        )
                    else:
                        results.append(
                            RemovalResult(
                                table_name=table_name,
                                semiring=semiring.name,
                                status="not_found",
                                message=f"No annotations found for table '{table_name}' with semiring '{semiring.name}'"
                            )
                        )
            except TableOrSchemaNotFoundError as e:
                logger.warning(f"Table or schema not found: {e}")
                raise HTTPException(
                    status.HTTP_404_NOT_FOUND,
                    detail=str(e)
                )
            except Exception as e:
                logger.error(
                    f"Failed to remove annotations from table '{table_name}'", exc_info=True)
                for semiring in semirings:
                    results.append(
                        RemovalResult(
                            table_name=table_name,
                            semiring=semiring.name,
                            status="error",
                            message=f"Error removing annotations from table '{table_name}' with semiring '{semiring.name}': {str(e)}"
                        )
                    )
                raise HTTPException(
                    status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to remove annotations from table '{table_name}': {str(e)}"
                )
        break  # Only process with first connection from pool

    return results


async def remove_annotation_ap_with_semiring(
    semiring_name: str,
    connection_string: ConnectionString,
    tables_names: TableNames,
    schema_name: SchemaName,
    all_semirings: List[DbSemiring] = Depends(get_semirings)
) -> List[RemovalResult]:
    """Remove annotations from AP tables for the chosen semiring using dynamic database connection."""
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

    logger.info(
        f"Removing annotations from tables: {tables_names} for semiring '{semiring_name}'"
    )
    results: List[RemovalResult] = []

    # Create the service with the connection string from the AP
    service_factory = get_provenance_service_for_ap(connection_string)

    # Use the factory to get the service
    async for prov_svc in service_factory():
        for table_name in tables_names:
            try:
                was_removed = await prov_svc.remove_annotation(table_name, schema_name, [semiring])
                if was_removed:
                    results.append(
                        RemovalResult(
                            table_name=table_name,
                            semiring=semiring.name,
                            status="success",
                            message=f"Annotations for table '{table_name}' with semiring '{semiring.name}' were successfully removed"
                        )
                    )
                else:
                    results.append(
                        RemovalResult(
                            table_name=table_name,
                            semiring=semiring.name,
                            status="not_found",
                            message=f"No annotations found for table '{table_name}' with semiring '{semiring.name}'"
                        )
                    )
            except TableOrSchemaNotFoundError as e:
                logger.warning(f"Table or schema not found: {e}")
                raise HTTPException(
                    status.HTTP_404_NOT_FOUND,
                    detail=str(e)
                )
            except Exception as e:
                logger.error(
                    f"Failed to remove annotations from table '{table_name}' with semiring '{semiring_name}'", exc_info=True)
                results.append(
                    RemovalResult(
                        table_name=table_name,
                        semiring=semiring.name,
                        status="error",
                        message=f"Error removing annotations from table '{table_name}' with semiring '{semiring.name}': {str(e)}"
                    )
                )
                raise HTTPException(
                    status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to remove annotations from table '{table_name}': {str(e)}"
                )
        break  # Only process with first connection from pool

    return results
