"""Task status polling for managed provenance tasks.

GET /aps/provenance/managed/tasks/{task_id}
"""
from typing import Any, Literal, Optional

from celery.result import AsyncResult
from pydantic import BaseModel

from ap_explanation.celery_app import celery_app

TaskStatus = Literal["pending", "started",
                     "success", "failure", "retry", "revoked"]


class TaskStatusResponse(BaseModel):
    task_id: str
    status: TaskStatus
    result: Optional[Any] = None
    error: Optional[str] = None


def get_managed_task_status(task_id: str) -> TaskStatusResponse:
    """Return the current status and (if completed) the result of a managed provenance task.

    Poll this endpoint after POST /aps/provenance/managed or
    POST /aps/provenance/managed/{semiring_name}.
    """
    async_result: AsyncResult = celery_app.AsyncResult(task_id)
    celery_status = (async_result.status or "pending").lower()

    if celery_status == "success":
        return TaskStatusResponse(
            task_id=task_id,
            status="success",
            result=async_result.result,
        )

    if celery_status == "failure":
        exc = async_result.result
        return TaskStatusResponse(
            task_id=task_id,
            status="failure",
            error=str(exc) if exc else "Unknown error",
        )

    mapped: TaskStatus = celery_status if celery_status in (  # type: ignore[assignment]
        "pending", "started", "retry", "revoked") else "pending"
    return TaskStatusResponse(task_id=task_id, status=mapped)
