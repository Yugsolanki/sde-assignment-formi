import logging

from src.tasks.celery_app import celery_app
from src.services.recording_poller import recording_poller

logger = logging.getLogger(__name__)


@celery_app.task(
    name="poll_recordings",
    bind=True,
)
def poll_recordings(self):
    """
    Periodic task to poll for pending recordings.

    Run every 30 seconds via Celery beat.
    """
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        uploaded = loop.run_until_complete(recording_poller.poll_pending_recordings())
        logger.info("recording_poll_task_complete", extra={"uploaded_count": uploaded})
    except Exception as e:
        logger.exception("recording_poll_task_error", extra={"error": str(e)})
    finally:
        loop.close()
