"""Manual explanation lifecycle – annotation management.

POST   /aps/explanation/manual/annotations               annotate with all semirings
POST   /aps/explanation/manual/annotations/{semiring}    annotate with one semiring
DELETE /aps/explanation/manual/annotations               remove all annotations
"""
from logging import getLogger
from typing import List, Literal

from fastapi import Depends, HTTPException, status
from pydantic import BaseModel

from ap_explanation.api.v1.dependencies.ap_parser import (
    DatabaseName,
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


class RemovalResult(BaseModel):
    table_name: str
    semiring: str
    status: Literal["success", "not_found", "error"]
    message: str


# ---------------------------------------------------------------------------
# POST /aps/provenance/manual/annotations
# ---------------------------------------------------------------------------

async def annotate_ap(
    db_name: DatabaseName,
    tables_names: TableNames,
    schema_name: SchemaName,
    semirings: List[DbSemiring] = Depends(get_semirings)
) -> List[AnnotationResult]:
    """Annotate AP tables with all available semirings.

    Part of the manual explanation lifecycle. After annotating, compute via
    POST /aps/explanation/manual/computations, then clean up via
    DELETE /aps/explanation/manual/annotations.
    """
    logger.info(f"Annotating tables: {tables_names} with all semirings")
    results: List[AnnotationResult] = []

    service_factory = get_provenance_service_for_ap(db_name)

    async for prov_svc in service_factory():
        for table_name in tables_names:
            try:
                was_annotated = await prov_svc.annotate_dataset(table_name, schema_name, semirings)
                for semiring in semirings:
                    if was_annotated:
                        results.append(AnnotationResult(
                            table_name=table_name,
                            semiring=semiring.name,
                            status="success",
                            message=f"Table '{table_name}' was successfully annotated with semiring '{semiring.name}'"
                        ))
                    else:
                        results.append(AnnotationResult(
                            table_name=table_name,
                            semiring=semiring.name,
                            status="already_annotated",
                            message=f"Table '{table_name}' is already annotated with semiring '{semiring.name}'"
                        ))
            except TableOrSchemaNotFoundError as e:
                logger.warning(f"Table or schema not found: {e}")
                raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(e))
            except Exception as e:
                logger.error(
                    f"Failed to annotate table '{table_name}'", exc_info=True)
                for semiring in semirings:
                    results.append(AnnotationResult(
                        table_name=table_name,
                        semiring=semiring.name,
                        status="error",
                        message=f"Error annotating table '{table_name}' with semiring '{semiring.name}': {str(e)}"
                    ))
                raise HTTPException(
                    status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to annotate table '{table_name}': {str(e)}"
                )
        break  # Only process with first connection from pool

    return results


# ---------------------------------------------------------------------------
# POST /aps/provenance/manual/annotations/{semiring_name}
# ---------------------------------------------------------------------------

async def annotate_ap_with_semiring(
    semiring_name: str,
    db_name: DatabaseName,
    tables_names: TableNames,
    schema_name: SchemaName,
    all_semirings: List[DbSemiring] = Depends(get_semirings)
) -> List[AnnotationResult]:
    """Annotate AP tables with a specific semiring.

    Part of the manual explanation lifecycle. After annotating, compute via
    POST /aps/explanation/manual/computations/{semiring_name}, then clean up via
    DELETE /aps/explanation/manual/annotations.
    """
    semiring = next(
        (s for s in all_semirings if s.name == semiring_name), None)
    if not semiring:
        available = ", ".join([s.name for s in all_semirings])
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"Semiring '{semiring_name}' not found. Available semirings: {available}"
        )

    logger.info(
        f"Annotating tables: {tables_names} with semiring '{semiring_name}'")
    results: List[AnnotationResult] = []

    service_factory = get_provenance_service_for_ap(db_name)

    async for prov_svc in service_factory():
        for table_name in tables_names:
            try:
                was_annotated = await prov_svc.annotate_dataset(table_name, schema_name, [semiring])
                if was_annotated:
                    results.append(AnnotationResult(
                        table_name=table_name,
                        semiring=semiring.name,
                        status="success",
                        message=f"Table '{table_name}' was successfully annotated with semiring '{semiring.name}'"
                    ))
                else:
                    results.append(AnnotationResult(
                        table_name=table_name,
                        semiring=semiring.name,
                        status="already_annotated",
                        message=f"Table '{table_name}' is already annotated with semiring '{semiring.name}'"
                    ))
            except TableOrSchemaNotFoundError as e:
                logger.warning(f"Table or schema not found: {e}")
                raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(e))
            except Exception as e:
                logger.error(
                    f"Failed to annotate table '{table_name}' with semiring '{semiring_name}'",
                    exc_info=True,
                )
                results.append(AnnotationResult(
                    table_name=table_name,
                    semiring=semiring.name,
                    status="error",
                    message=f"Error annotating table '{table_name}' with semiring '{semiring.name}': {str(e)}"
                ))
                raise HTTPException(
                    status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Error annotating table '{table_name}' with semiring '{semiring_name}': {str(e)}"
                )
        break  # Only process with first connection from pool

    return results


# ---------------------------------------------------------------------------
# DELETE /aps/provenance/manual/annotations
# ---------------------------------------------------------------------------

async def remove_annotation_ap(
    db_name: DatabaseName,
    tables_names: TableNames,
    schema_name: SchemaName,
    semirings: List[DbSemiring] = Depends(get_semirings)
) -> List[RemovalResult]:
    """Remove provenance annotations from AP tables.

    Part of the manual explanation lifecycle. Call this after
    POST /aps/explanation/manual/computations to clean up annotations.
    """
    logger.info(
        f"Removing annotations from tables: {tables_names} for all semirings")
    results: List[RemovalResult] = []

    service_factory = get_provenance_service_for_ap(db_name)

    async for prov_svc in service_factory():
        for table_name in tables_names:
            try:
                was_removed = await prov_svc.remove_annotation(table_name, schema_name)
                for semiring in semirings:
                    if was_removed:
                        results.append(RemovalResult(
                            table_name=table_name,
                            semiring=semiring.name,
                            status="success",
                            message=f"Annotations for table '{table_name}' with semiring '{semiring.name}' were successfully removed"
                        ))
                    else:
                        results.append(RemovalResult(
                            table_name=table_name,
                            semiring=semiring.name,
                            status="not_found",
                            message=f"No annotations found for table '{table_name}' with semiring '{semiring.name}'"
                        ))
            except TableOrSchemaNotFoundError as e:
                logger.warning(f"Table or schema not found: {e}")
                raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(e))
            except Exception as e:
                logger.error(
                    f"Failed to remove annotations from table '{table_name}'", exc_info=True)
                for semiring in semirings:
                    results.append(RemovalResult(
                        table_name=table_name,
                        semiring=semiring.name,
                        status="error",
                        message=f"Error removing annotations from table '{table_name}' with semiring '{semiring.name}': {str(e)}"
                    ))
                raise HTTPException(
                    status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to remove annotations from table '{table_name}': {str(e)}"
                )
        break  # Only process with first connection from pool

    return results
