from logging import getLogger
from typing import List, Literal

from fastapi import Depends, HTTPException, status
from pydantic import BaseModel

from ap_explanation.api.v1.dependencies.ap_parser import (
    ConnectionString,
    SchemaName,
    TableNames,
)
from ap_explanation.di import get_provenance_service_for_ap, get_semirings
from ap_explanation.errors import TableOrSchemaNotFoundError
from ap_explanation.types.semiring import DbSemiring

logger = getLogger(__name__)


class AnnotationResult(BaseModel):
    table_name: str
    semiring: str
    status: Literal["success", "already_annotated", "error"]
    message: str


async def annotate_ap_with_semiring(
    semiring_name: str,
    connection_string: ConnectionString,
    tables_names: TableNames,
    schema_name: SchemaName,
    all_semirings: List[DbSemiring] = Depends(get_semirings)
) -> List[AnnotationResult]:
    """Annotate the AP with the chosen semiring using dynamic database connection."""
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
        f"Annotating tables: {tables_names} with semiring '{semiring_name}'"
    )
    results: List[AnnotationResult] = []

    # Create the service with the connection string from the AP
    service_factory = get_provenance_service_for_ap(connection_string)

    # Use the factory to get the service
    async for prov_svc in service_factory():
        for table_name in tables_names:
            try:
                was_annotated = await prov_svc.annotate_dataset(table_name, schema_name, [semiring])
                if was_annotated:
                    results.append(
                        AnnotationResult(
                            table_name=table_name,
                            semiring=semiring.name,
                            status="success",
                            message=f"Table '{table_name}' was successfully annotated with semiring '{semiring.name}'"
                        )
                    )
                else:
                    results.append(
                        AnnotationResult(
                            table_name=table_name,
                            semiring=semiring.name,
                            status="already_annotated",
                            message=f"Table '{table_name}' is already annotated with semiring '{semiring.name}'"
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
                    f"Failed to annotate table '{table_name}' with semiring '{semiring_name}'", exc_info=True)
                results.append(
                    AnnotationResult(
                        table_name=table_name,
                        semiring=semiring.name,
                        status="error",
                        message=f"Error annotating table '{table_name}' with semiring '{semiring.name}': {str(e)}"
                    )
                )
                raise HTTPException(
                    status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Error annotating table '{table_name}' with semiring '{semiring_name}': {str(e)}"
                )
        break  # Only process with first connection from pool

    return results
