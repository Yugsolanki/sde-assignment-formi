from celery import Celery
from celery.schedules import crontab

from src.config import settings

celery_app = Celery(
    "voicebot",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_default_queue=settings.POSTCALL_CELERY_QUEUE,
    broker_connection_retry_on_startup=True,
    imports=["src.tasks.celery_tasks"],
    # Beat schedule for periodic tasks
    beat_schedule={
        "poll-recordings": {
            "task": "poll_recordings",
            "schedule": 30.0,  # Every 30 seconds
        },
        "schedule-postcall-tasks": {
            "task": "schedule_postcall_tasks",
            "schedule": 5.0,  # Every 5 seconds
        },
        "recover-stale-tasks": {
            "task": "recover_stale_tasks",
            "schedule": 300.0,  # Every 5 minutes
        },
        "alert-dead-letter-tasks": {
            "task": "alert_dead_letter_tasks",
            "schedule": crontab(minute=0),  # Every hour
        },
    },
)
