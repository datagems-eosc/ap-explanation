from logging import getLogger
from typing import Never

from fastapi import Depends, Response, status

from ap_explanation.middlewares.auth import require_authentication
from ap_explanation.tasks.maintenance import circuit_cleanup_task

from .managed import ManagedProvenanceTaskResponse

logger = getLogger(__name__)


def circuit_cleanup(
    db_name: str,
    response: Response,
    _auth: Never = Depends(require_authentication()),
) -> ManagedProvenanceTaskResponse:
    """Dispatch an async task reclaiming ProvSQL's circuit store on a database.

    Runs `provsql.circuit_cleanup()`, which keeps the gates still referenced by
    a token stored in the database. It waits for the explain tasks running on
    the database, and fails if other clients stay connected to it.

    Returns HTTP 202 Accepted. Poll the result at GET /api/v1/aps/explanation/{task_id}.
    """
    logger.info(f"Dispatching circuit cleanup task for database '{db_name}'")
    # NOTE: Type of celery tasks must be ignored, as celery annotation monkey patches the function object
    task = circuit_cleanup_task.delay(db_name)  # type: ignore
    response.status_code = status.HTTP_202_ACCEPTED
    return ManagedProvenanceTaskResponse(task_id=task.id)
