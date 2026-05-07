import logging
from datetime import datetime, timedelta

from sqlalchemy import select, update, and_

from src.tasks.celery_app import celery_app
from src.utils.db import async_session_factory
from src.models.postcall_task import PostCallTask, TaskStatus

logger = logging.getLogger(__name__)


@celery_app.task(
    name="recover_stale_tasks",
    bind=True,
)
def recover_stale_tasks(self):
    """
    Recovery task to reset tasks stuck in PROCESSING state.

    Runs every 5 minutes to recover from worker crashes.
    """
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        recovered = loop.run_until_complete(_recover_stale())
        if recovered > 0:
            logger.warning("stale_tasks_recovered", extra={"count": recovered})
    except Exception as e:
        logger.exception("recovery_task_error", extra={"error": str(e)})
    finally:
        loop.close()


async def _recover_stale() -> int:
    """Find and reset tasks stuck in PROCESSING."""
    stale_threshold = datetime.utcnow() - timedelta(minutes=10)

    async with async_session_factory() as session:
        # Find tasks stuck in PROCESSING for > 10 minutes
        stmt = select(PostCallTask).where(
            and_(
                PostCallTask.status == TaskStatus.PROCESSING.value,
                PostCallTask.started_at < stale_threshold,
            )
        )

        result = await session.execute(stmt)
        stale_tasks = result.scalars().all()

        if not stale_tasks:
            return 0

        # Reset to QUEUED
        task_ids = [t.id for t in stale_tasks]
        await session.execute(
            update(PostCallTask)
            .where(PostCallTask.id.in_(task_ids))
            .values(
                status=TaskStatus.QUEUED.value,
                started_at=None,
                version=PostCallTask.version + 1,
                updated_at=datetime.utcnow(),
            )
        )

        await session.commit()

        # Log each recovery
        for task in stale_tasks:
            task.add_error(
                f"Recovered from stale PROCESSING state (started_at={task.started_at})"
            )
            logger.info(
                "task_recovered",
                extra={
                    "task_id": str(task.id),
                    "interaction_id": str(task.interaction_id),
                    "stale_since": (
                        task.started_at.isoformat() if task.started_at else None
                    ),
                },
            )

        return len(stale_tasks)


@celery_app.task(
    name="alert_dead_letter_tasks",
    bind=True,
)
def alert_dead_letter_tasks(self):
    """
    Alert on tasks in FAILED state.

    Runs hourly to ensure no tasks are silently failing.
    """
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        failed_count = loop.run_until_complete(_check_dead_letter())
        if failed_count > 0:
            logger.error(
                "dead_letter_tasks_found",
                extra={"count": failed_count},
                extra={"alert": "dead_letter"},
            )
    except Exception as e:
        logger.exception("dead_letter_check_error", extra={"error": str(e)})
    finally:
        loop.close()


async def _check_dead_letter() -> int:
    """Count tasks in FAILED state."""
    async with async_session_factory() as session:
        from sqlalchemy import func

        stmt = select(func.count()).where(
            PostCallTask.status == TaskStatus.FAILED.value
        )

        result = await session.execute(stmt)
        return result.scalar()
