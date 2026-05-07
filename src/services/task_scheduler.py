import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List

from sqlalchemy import select, and_, func, update

from src.models.customer_config import CustomerConfig
from src.utils.db import async_session_factory
from src.models.postcall_task import (
    PostCallTask,
    TaskStatus,
    PriorityClass,
    RecordingStatus,
)
from src.services.rate_limiter import token_rate_limiter, RateLimitExceeded
from src.config import settings

logger = logging.getLogger(__name__)


class TaskScheduler:
    """
    Priority-aware task scheduler that respects rate limits.

    Scheduling Logic:
    1. Always process P0 tasks first if any budget available
    2. Process P1 tasks if utilization < 80%
    3. Process P2 tasks if utilization < 50%
    4. Defer tasks if rate limit would be exceeded
    """

    BATCH_SIZE = 20
    UTILIZATION_THRESHOLDS = {
        PriorityClass.HIGH: 100,  # Always try P0
        PriorityClass.NORMAL: 80,  # P1 if < 80% utilized
        PriorityClass.LOW: 50,  # P2 if < 50% utilized
    }

    async def schedule_tasks(self) -> int:
        """
        Pick up an process ready tasks according to priority and rate limit
        """
        started_count = 0

        # check current situation
        utilization = await token_rate_limiter.get_utilization_percentage()

        # Determine which priority classes to process
        priorities_to_process = []
        for priority, threshold in self.UTILIZATION_THRESHOLDS.items():
            if utilization <= threshold:
                priorities_to_process.append(priority.value)

        if not priorities_to_process:
            logger.debug(
                "scheduler_deferred_high_utilization",
                extra={"utilization": round(utilization, 2)},
            )
            return 0

        logger.debug(
            "scheduler_priorities",
            extra={
                "utilization": round(utilization, 2),
                "priorities": priorities_to_process,
            },
        )

        # Process tasks by priority order
        for priority in sorted(priorities_to_process):
            batch = await self._claim_tasks(priority, self.BATCH_SIZE - started_count)

            for task in batch:
                try:
                    success = await self._process_task(task)
                    if success:
                        started_count += 1
                except Exception as e:
                    logger.exception(
                        "task_processing_error",
                        extra={
                            "task_id": str(task.id),
                            "interaction_id": str(task.interaction_id),
                            "error": str(e),
                        },
                    )
        return started_count

    async def _claim_tasks(self, priority_class: int, limit: int) -> List[PostCallTask]:
        """
        Claim tasks of given priority for processing.

        Uses UPDATE with RETURNING for atomic claim with FOR UPDATE SKIP LOCKED
        to handle concurrent schedulers.
        """
        if limit <= 0:
            return []

        async with async_session_factory() as session:
            subq = (
                select(PostCallTask.id)
                .where(
                    and_(
                        PostCallTask.status.in_(
                            [TaskStatus.QUEUED.value, TaskStatus.DEFERRED.value]
                        ),
                        PostCallTask.priority_class == priority_class,
                        PostCallTask.scheduled_at <= datetime.now(timezone.utc),
                    )
                )
                .limit(limit)
                .with_for_update(skip_locked=True)
                .scalar_subquery()
            )

            stmt = (
                update(PostCallTask)
                .where(PostCallTask.id.in_(subq))
                .values(
                    status=TaskStatus.PROCESSING.value,
                    started_at=datetime.now(timezone.utc),
                    version=PostCallTask.version + 1,
                )
                .returning(PostCallTask)
                .execution_options()
                .limit(limit)
            )

            result = await session.execute(stmt)
            tasks = result.scalars().all()

            if tasks:
                await session.commit()
                logger.info(
                    "tasks_claimed",
                    extra={
                        "count": len(tasks),
                        "priority": priority_class,
                    },
                )
                return tasks

    async def _process_task(self, task: PostCallTask) -> bool:
        """
        Process a single task: acquire budget, run LLM, update status.

        Returns:
            True if processing started, False if deferred
        """
        # Load customer config
        # Load customer config
        customer_config = await self._get_customer_config(task.customer_id)
        if not customer_config:
            # Use default config if not set
            customer_config = CustomerConfig(
                customer_id=task.customer_id,
                token_budget_per_minute=settings.LLM_TOKENS_PER_MINUTE // 10,
                priority_boost=1.0,
            )

        # Try to acquire budget
        try:
            await token_rate_limiter.acquire_budget(
                customer_id=str(task.customer_id),
                customer_config=customer_config,
                estimated_tokens=task.estimated_tokens,
            )
        except RateLimitExceeded as e:
            # Defer task to next minute
            await self._defer_task(task, reason=e.reason)
            logger.info(
                "task_deferred_rate_limit",
                extra={
                    "task_id": str(task.id),
                    "interaction_id": str(task.interaction_id),
                    "reason": e.reason,
                    "priority": task.priority_class,
                },
            )
            return False

        # Budget acquired - task is already in PROCESSING state
        # The actual LLM call will be made by the caller
        logger.info(
            "task_ready_for_llm",
            extra={
                "task_id": str(task.id),
                "interaction_id": str(task.interaction_id),
                "priority": task.priority_class,
                "tokens_budgeted": task.estimated_tokens,
            },
        )

        return True

    async def _defer_task(self, task: PostCallTask, reason: str) -> None:
        """Defer a task to next minute"""
        async with async_session_factory() as session:
            next_minute = datetime.now(timezone.utc).replace(
                second=0, microsecond=0
            ) + timedelta(minutes=1)

            await session.execute(
                update(PostCallTask)
                .where(PostCallTask.id == task.id)
                .values(
                    status=TaskStatus.DEFERRED.value,
                    scheduled_at=next_minute,
                    version=PostCallTask.version + 1,
                )
            )
            await session.commit()

    async def _get_customer_config(self, customer_id) -> Optional[CustomerConfig]:
        """Load customer configuration"""
        async with async_session_factory() as session:
            stmt = select(CustomerConfig).where(
                CustomerConfig.customer_id == customer_id
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def get_queue_metrics(self) -> dict:
        """Get current queue metrics for monitoring"""
        async with async_session_factory() as session:
            metrics = {}

            # Count by status
            for status in TaskStatus:
                stmt = select(func.count()).where(PostCallTask.status == status.value)
                result = await session.execute(stmt)
                metrics[f"status_{status.value.lower()}"] = result.scalar()

            for priority in PriorityClass:
                stmt = select(func.count()).where(
                    and_(
                        PostCallTask.priority_class == priority.value,
                        PostCallTask.status.in_(
                            [TaskStatus.QUEUED.value, TaskStatus.DEFERRED.value]
                        ),
                    )
                )
                result = await session.execute(stmt)
                metrics[f"priority_{priority.name.lower()}"] = result.scalar()

            # Recording status
            for status in RecordingStatus:
                stmt = select(func.count()).where(
                    PostCallTask.recording_status == status.value
                )
                result = await session.execute(stmt)
                metrics[f"recording_{status.value.lower()}"] = result.scalar()

            return metrics


task_scheduler = TaskScheduler()
