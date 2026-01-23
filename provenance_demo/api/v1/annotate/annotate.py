from logging import getLogger
from typing import List, Literal

from fastapi import Depends, HTTPException, status
from pydantic import BaseModel

from provenance_demo.api.v1.dependencies.ap_parser import SchemaName, TableNames
from provenance_demo.di import get_provenance_service, get_semirings
from provenance_demo.services.provenance import ProvenanceService
from provenance_demo.types.semiring import DbSemiring

logger = getLogger(__name__)


class AnnotationResult(BaseModel):
    table_name: str
    semiring: str
    status: Literal["success", "already_annotated", "error"]
    message: str


async def annotate_ap(
    tables_names: TableNames,
    schema_name: SchemaName,
    prov_svc: ProvenanceService = Depends(get_provenance_service),
    semirings: List[DbSemiring] = Depends(get_semirings)
) -> List[AnnotationResult]:
    """Annotate the AP with all available semirings."""
    logger.info(f"Annotating tables: {tables_names} with all semirings")
    results: List[AnnotationResult] = []

    for table_name in tables_names:
        try:
            was_annotated = await prov_svc.annotate_dataset(table_name, schema_name, semirings)
            for semiring in semirings:
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
        except Exception as e:
            logger.error(
                f"Failed to annotate table '{table_name}'", exc_info=True)
            for semiring in semirings:
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
                detail=f"Error annotating table '{table_name}': {str(e)}"
            )

    return results
