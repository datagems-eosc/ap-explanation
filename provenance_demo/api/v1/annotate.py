from asyncio import gather
from json import loads
from logging import getLogger
from typing import List, Literal

from fastapi import Depends, HTTPException, status
from pydantic import BaseModel

from provenance_demo.di import get_provenance_service

# TODO Reexport ProvenanceService in di.py?
from provenance_demo.services.provenance import ProvenanceService
from provenance_demo.types.pg_json import PgJson

logger = getLogger(__name__)


class AnnotationResult(BaseModel):
    table_name: str
    status: Literal["success", "already_annotated", "error"]
    message: str


async def annotate(ap: PgJson, prov_svc: ProvenanceService = Depends(get_provenance_service)) -> List[AnnotationResult]:
    # TODO: Remove hardcoded schema name
    schema_name = "mathe"
    tables_nodes = ap.get_nodes_by_label("Table")
    if not tables_nodes or len(tables_nodes) == 0:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="This AP has no Table nodes!"
        )

    tables_names = [node.properties["name"]
                    for node in tables_nodes if node.properties and "name" in node.properties]
    if len(tables_names) != len(tables_nodes):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Some Table nodes are missing the 'name' property!"
        )

    logger.info(f"Annotating tables: {tables_names}")
    results: List[AnnotationResult] = []

    for table_name in tables_names:
        try:
            was_annotated = await prov_svc.annotate_dataset(table_name, schema_name)
            if was_annotated:
                results.append(
                    AnnotationResult(
                        table_name=table_name,
                        status="success",
                        message=f"Table '{table_name}' was successfully annotated"
                    )
                )
            else:
                results.append(
                    AnnotationResult(
                        table_name=table_name,
                        status="already_annotated",
                        message=f"Table '{table_name}' is already annotated"
                    )
                )
        except Exception as e:
            logger.error(
                f"Failed to annotate table '{table_name}'", exc_info=True)
            results.append(
                AnnotationResult(
                    table_name=table_name,
                    status="error",
                    message=f"Error annotating table '{table_name}': {str(e)}"
                )
            )
            raise HTTPException(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error annotating table '{table_name}': {str(e)}"
            )

    return results
