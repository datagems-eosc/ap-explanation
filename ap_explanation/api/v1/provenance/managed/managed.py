from enum import StrEnum
from logging import getLogger
from typing import Never

from fastapi import Depends, Response, status
from pydantic import BaseModel

from ap_explanation.middlewares.auth import require_authentication
from ap_explanation.semirings import semirings
from ap_explanation.tasks.explain import explain_task
from ap_explanation.types.provenance_analytical_pattern import (
    ProvenanceAnalyticalPattern,
)

SemiringName = StrEnum("SemiringName", {s.name: s.name for s in semirings})

logger = getLogger(__name__)


class ManagedProvenanceTaskResponse(BaseModel):
    task_id: str
    status: str = "pending"


def managed_provenance_ap(
    ap: ProvenanceAnalyticalPattern,
    response: Response,
    _auth: Never = Depends(require_authentication())
) -> ManagedProvenanceTaskResponse:
    """Dispatch an async task for the full explanation lifecycle with all semirings.

    Returns HTTP 202 Accepted. Poll the result at GET /api/v1/aps/explanation/{task_id}.
    """
    ds = ap.data_source
    logger.info(
        f"Dispatching managed provenance task for tables: {ds.table_names} with all semirings")
    # NOTE: Type of celery tasks must be ignored, as celery annotation monkey patches the function object
    task = explain_task.delay(ap.model_dump(mode="json"))  # type: ignore # noqa
    response.status_code = status.HTTP_202_ACCEPTED
    return ManagedProvenanceTaskResponse(task_id=task.id)


def managed_provenance_ap_with_semiring(
    semiring_name: SemiringName,
    ap: ProvenanceAnalyticalPattern,
    response: Response,
    _auth: Never = Depends(require_authentication()),
) -> ManagedProvenanceTaskResponse:
    """Dispatch an async task for the full explanation lifecycle with a specific semiring.

    Returns HTTP 202 Accepted. Poll the result at GET /api/v1/aps/explanation/{task_id}.
    """
    ds = ap.data_source
    logger.info(
        f"Dispatching managed provenance task for tables: {ds.table_names} with semiring '{semiring_name}'"
    )
    # NOTE: Type of celery tasks must be ignored, as celery annotation monkey patches the function object
    task = explain_task.delay(ap.model_dump(mode="json"), semiring_name)  # type: ignore # noqa
    response.status_code = status.HTTP_202_ACCEPTED
    return ManagedProvenanceTaskResponse(task_id=task.id)
