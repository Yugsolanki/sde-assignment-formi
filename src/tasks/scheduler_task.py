import logging

from src.tasks.celery_app import celery_app
from src.tasks.llm_executor_task import execute_llm_analysis
from src.services.task_scheduler import task_scheduler

logger = logging.getLogger(__name__)


@celery_app.task(
    name="schedule_postcall_tasks",
    bind=True,
)
def schedule_postcall_tasks(self):
    """
    Worker task that schedules and dispatches post-call processing.

    This replaces the old direct Celery task dispatch.
    Now it:
    1. Claims tasks according to priority
    2. Checks rate limits
    3. Spawns LLM executor tasks for ready tasks
    """
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        started = loop.run_until_complete(_schedule_and_dispatch())
        logger.info("scheduler_task_complete", extra={"tasks_started": started})
    except Exception as e:
        logger.exception("scheduler_task_error", extra={"error": str(e)})
    finally:
        loop.close()


async def _schedule_and_dispatch() -> int:
    """Schedule tasks and dispatch to LLM executors."""

    # Get queue metrics for logging
    metrics = await task_scheduler.get_queue_metrics()
    logger.info("queue_metrics", extra=metrics)

    # Claim tasks according to priority and rate limits
    # This returns tasks that have been moved to PROCESSING state
    # and have budget acquired
    started_count = await task_scheduler.schedule_tasks()

    if started_count == 0:
        return 0

    # Find tasks in PROCESSING state that need LLM execution
    from src.utils.db import async_session_factory
    from src.models.postcall_task import PostCallTask, TaskStatus
    from sqlalchemy import select

    async with async_session_factory() as session:
        stmt = (
            select(PostCallTask)
            .where(PostCallTask.status == TaskStatus.PROCESSING.value)
            .limit(started_count)
        )

        result = await session.execute(stmt)
        tasks = result.scalars().all()

    # Spawn LLM executor tasks
    for task in tasks:
        execute_llm_analysis.delay(str(task.id))

    return len(tasks)
