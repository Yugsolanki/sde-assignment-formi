import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select, update

from src.tasks.celery_app import celery_app
from src.utils.db import async_session_factory
from src.models.postcall_task import PostCallTask, TaskStatus
from src.models.interaction import Interaction
from src.models.llm_usage_log import LLMUsageLog
from src.services.post_call_processor import PostCallProcessor, PostCallContext
from src.services.rate_limiter import token_rate_limiter
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage

logger = logging.getLogger(__name__)


@celery_app.task(
    name="execute_llm_analysis",
    bind=True,
    max_retries=5,
    default_retry_delay=60,
    acks_late=True,
    queue="postcall_processing",
)
def execute_llm_analysis(task_id: str):
    """
    Execute LLM analysis for a post-call task.

    This task is spawned by the scheduler after budget is acquired.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_execute_analysis(task_id))
    except Exception as e:
        logger.exception(
            "llm_executor_failed",
            extra={"task_id": task_id, "error": str(e)},
        )
        # Let Celery handle retries
        raise
    finally:
        loop.close()


async def _execute_analysis(task_id: str):
    """Execute the actual LLM analysis."""

    # Load task
    async with async_session_factory() as session:
        stmt = select(PostCallTask).where(PostCallTask.id == task_id)
        result = await session.execute(stmt)
        task = result.scalar_one_or_none()

        if not task:
            logger.error("task_not_found", extra={"task_id": task_id})
            return

        if task.status != TaskStatus.PROCESSING.value:
            logger.warning(
                "task_not_in_processing",
                extra={"task_id": task_id, "status": task.status},
            )
            return

    # Load interaction
    async with async_session_factory() as session:
        stmt = select(Interaction).where(Interaction.id == task.interaction_id)
        result = await session.execute(stmt)
        interaction = result.scalar_one_or_none()

        if not interaction:
            await _fail_task(task, "interaction_not_found")
            return

    # Check recording status - wait for READY if needed
    if task.recording_status == "PENDING":
        # Don't fail, just log - recording poller will handle it
        logger.info(
            "analysis_proceeding_without_recording",
            extra={
                "task_id": task_id,
                "interaction_id": str(task.interaction_id),
            },
        )

    # Build context
    ctx = PostCallContext(
        interaction_id=str(task.interaction_id),
        session_id=str(interaction.session_id),
        lead_id=str(interaction.lead_id),
        campaign_id=str(interaction.campaign_id),
        customer_id=str(task.customer_id),
        agent_id=str(interaction.agent_id),
        call_sid=interaction.call_sid or "",
        transcript_text=interaction.transcript_text,
        conversation_data=interaction.conversation_data or {},
        additional_data={},
        ended_at=interaction.ended_at or datetime.now(timezone.utc),
        exotel_account_id=interaction.exotel_account_id,
    )

    # Execute LLM analysis
    processor = PostCallProcessor()

    try:
        result = await processor.process_post_call(ctx, single_prompt=True)

        # Adjust token budget based on actual usage
        await token_rate_limiter.record_actual_usage(
            customer_id=str(task.customer_id),
            estimated_tokens=task.estimated_tokens,
            actual_tokens=result.tokens_used,
        )

        # Log usage for billing
        await _log_usage(task, interaction, result)

        # Update task
        async with async_session_factory() as session:
            await session.execute(
                update(PostCallTask)
                .where(PostCallTask.id == task.id)
                .values(
                    status=TaskStatus.COMPLETED.value,
                    completed_at=datetime.now(timezone.utc),
                    actual_tokens=result.tokens_used,
                    version=PostCallTask.version + 1,
                    updated_at=datetime.now(timezone.utc),
                )
            )

            # Update interaction metadata
            await session.execute(
                update(Interaction)
                .where(Interaction.id == task.interaction_id)
                .values(
                    interaction_metadata={
                        **(interaction.interaction_metadata or {}),
                        "call_stage": result.call_stage,
                        "entities": result.entities,
                        "summary": result.summary,
                        "analysis_status": "completed",
                    },
                    processing_status="COMPLETED",
                )
            )

            await session.commit()

        logger.info(
            "postcall_analysis_complete",
            extra={
                "task_id": task_id,
                "interaction_id": str(task.interaction_id),
                "customer_id": str(task.customer_id),
                "call_stage": result.call_stage,
                "tokens_used": result.tokens_used,
                "latency_ms": result.latency_ms,
                "priority": task.priority_class,
            },
        )

        # Trigger downstream actions
        await _trigger_downstream(task, interaction, result)

    except Exception as e:
        logger.exception(
            "postcall_analysis_error",
            extra={
                "task_id": task_id,
                "interaction_id": str(task.interaction_id),
                "error": str(e),
            },
        )
        await _fail_task(task, str(e))


async def _fail_task(task: PostCallTask, error: str):
    """Mark task as failed."""
    async with async_session_factory() as session:
        task.retry_count += 1
        task.add_error(error)

        if task.retry_count >= task.max_retries:
            task.status = TaskStatus.FAILED.value
            task.completed_at = datetime.now(timezone.utc)

            logger.error(
                "task_failed_permanently",
                extra={
                    "task_id": str(task.id),
                    "interaction_id": str(task.interaction_id),
                    "retry_count": task.retry_count,
                    "errors": task.error_log,
                },
            )
        else:
            # Reset to QUEUED for retry
            task.status = TaskStatus.QUEUED.value
            task.started_at = None

        task.updated_at = datetime.now(timezone.utc)
        task.version += 1

        await session.commit()


async def _log_usage(task: PostCallTask, interaction: Interaction, result):
    """Log LLM usage for billing."""
    usage_log = LLMUsageLog(
        interaction_id=task.interaction_id,
        customer_id=task.customer_id,
        campaign_id=interaction.campaign_id,
        tokens_used=result.tokens_used,
        latency_ms=int(result.latency_ms),
        call_stage=result.call_stage,
        model=result.model,
        provider=result.provider,
    )

    async with async_session_factory() as session:
        session.add(usage_log)
        await session.commit()


async def _trigger_downstream(task: PostCallTask, interaction: Interaction, result):
    """Trigger downstream actions after successful analysis."""
    try:
        await trigger_signal_jobs(
            interaction_id=str(task.interaction_id),
            session_id=str(interaction.session_id),
            campaign_id=str(interaction.campaign_id),
            analysis_result=result.raw_response,
        )
    except Exception as e:
        logger.warning(
            "signal_jobs_failed",
            extra={"interaction_id": str(task.interaction_id), "error": str(e)},
        )

    try:
        await update_lead_stage(
            lead_id=str(interaction.lead_id),
            interaction_id=str(task.interaction_id),
            call_stage=result.call_stage,
        )
    except Exception as e:
        logger.warning(
            "lead_stage_update_failed",
            extra={"interaction_id": str(task.interaction_id), "error": str(e)},
        )

    # Update downstream_triggers in task
    async with async_session_factory() as session:
        triggers = task.downstream_triggers or {}
        triggers["signal_jobs"] = {
            "status": "completed",
            "at": datetime.now(timezone.utc).isoformat(),
        }
        triggers["lead_stage"] = {
            "status": "completed",
            "at": datetime.now(timezone.utc).isoformat(),
        }

        await session.execute(
            update(PostCallTask)
            .where(PostCallTask.id == task.id)
            .values(downstream_triggers=triggers)
        )
        await session.commit()
