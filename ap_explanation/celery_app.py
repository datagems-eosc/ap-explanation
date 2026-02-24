"""
This is the entrypoint for a new worker process that runs AP explanation tasks. 
"""
from celery import Celery
from dotenv import load_dotenv

from ap_explanation.di import REDIS_BROKER_URI

load_dotenv()

# A standlaone celery worker can be run with :
# docker run --rm \
#   --env-file .env \
#   ap-explanation:prod \
#   uv run celery -A ap_explanation.celery_app:celery_app worker --loglevel=info


celery_app = Celery(
    "ap_explanation",
    broker=REDIS_BROKER_URI,
    backend=REDIS_BROKER_URI,
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
