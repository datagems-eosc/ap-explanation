"""Managed provenance lifecycle – full annotate → compute → cleanup, asynchronously.

POST /aps/provenance/managed                dispatch task with all semirings
POST /aps/provenance/managed/{semiring}     dispatch task with one semiring
GET  /aps/provenance/managed/tasks/{id}     poll task status / retrieve result
"""
from logging import getLogger

from fastapi import Response, status
from pydantic import BaseModel

from ap_explanation.api.v1.dependencies.ap_parser import (
    DatabaseName,
    SchemaName,
    SqlOperator,
    TableNames,
)
from ap_explanation.tasks.explain import explain_task

logger = getLogger(__name__)


class ManagedProvenanceTaskResponse(BaseModel):
    task_id: str
    status: str = "pending"


def managed_provenance_ap(
    db_name: DatabaseName,
    sql_node: SqlOperator,
    schema_name: SchemaName,
    tables_names: TableNames,
    response: Response,
) -> ManagedProvenanceTaskResponse:
    """Dispatch an async task that handles the full provenance lifecycle with all semirings:
    annotate → compute provenance → remove annotation.

    Returns HTTP 202 Accepted. Poll the result at
    GET /api/v1/aps/provenance/managed/tasks/{task_id}.
    """
    query = sql_node.properties["query"] if sql_node.properties else ""
    logger.info(
        f"Dispatching managed provenance task for tables: {tables_names} with all semirings")
    # NOTE: Type of celery tasks must be ignored, as celery annotation monkey patches the function object
    task = explain_task.delay(db_name, list(tables_names), schema_name, query)  # type: ignore # noqa
    response.status_code = status.HTTP_202_ACCEPTED
    return ManagedProvenanceTaskResponse(task_id=task.id)


def managed_provenance_ap_with_semiring(
    semiring_name: str,
    db_name: DatabaseName,
    sql_node: SqlOperator,
    schema_name: SchemaName,
    tables_names: TableNames,
    response: Response,
) -> ManagedProvenanceTaskResponse:
    """Dispatch an async task that handles the full provenance lifecycle for a specific semiring:
    annotate → compute provenance → remove annotation.

    Returns HTTP 202 Accepted. Poll the result at
    GET /api/v1/aps/provenance/managed/tasks/{task_id}.
    """
    query = sql_node.properties["query"] if sql_node.properties else ""
    logger.info(
        f"Dispatching managed provenance task for tables: {tables_names} with semiring '{semiring_name}'"
    )
    # NOTE: Type of celery tasks must be ignored, as celery annotation monkey patches the function object
    task = explain_task.delay(db_name, list(tables_names), schema_name, query, semiring_name)  # type: ignore # noqa
    response.status_code = status.HTTP_202_ACCEPTED
    return ManagedProvenanceTaskResponse(task_id=task.id)
