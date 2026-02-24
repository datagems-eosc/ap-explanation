from os import getenv

from celery import Celery
from dotenv import load_dotenv

load_dotenv()

celery_app = Celery(
    "ap_explanation",
    broker=getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
    include=["ap_explanation.tasks.explain"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    result_expires=3600,  # Results are kept for 1 hour
)
