import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, List

import httpx
from sqlalchemy import select, and_

from src.config import settings
from src.utils.db import async_session_factory
from src.models.postcall_task import PostCallTask, RecordingStatus, TaskStatus
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)


class RecordingPoller:
    """
    Polls for call recordings with exponential backoff.

    Replaces the asyncio.sleep(45s) approach with a robust polling
    mechanism that:
    - Retries with exponential backoff (30s, 60s, 120s, ... max 1h)
    - Tracks polling state in database
    - Emits structured logs for every poll attempt
    - Alerts on failures
    - Never silently skips recordings
    """

    MAX_RETRIES = 10
    BASE_DELAY_SECONDS = 30
    MAX_DELAY_SECONDS = 3600
    BATCH_SIZE = 50

    async def poll_pending_recordings(self) -> int:
        """
        Poll recordings for all tasks with PENDING status.

        Returns:
            Number of recordings successfully uploaded
        """
        uploaded_count = 0

        async with async_session_factory() as session:
            stmt = (
                select(PostCallTask)
                .where(
                    and_(
                        PostCallTask.recording_status == RecordingStatus.PENDING.value,
                        PostCallTask.next_poll_at <= datetime.now(),
                    )
                )
                .limit(self.BATCH_SIZE)
                .with_for_update(skip_locked=True)
            )

            result = await session.execute(stmt)
            tasks = result.scalars().all()

            if not tasks:
                return 0

            logger.info("recording_poll_batch_start", extra={"batch_size": len(tasks)})

            for task in tasks:
                try:
                    interaction = await self._load_interaction(
                        session, task.interaction_id
                    )
                    if not interaction:
                        await self._mark_recording_failed(
                            session, task, "interaction_not_found"
                        )
                        continue
                    call_sid = interaction.call_sid
                    exotel_account_id = interaction.exotel_account_id

                    if not call_sid or not exotel_account_id:
                        logger.warning(
                            "recording_skip_missing_metadata",
                            extra={
                                "interaction_id": str(task.interaction_id),
                                "has_call_sid": bool(call_sid),
                                "has_exotel_account": bool(exotel_account_id),
                            },
                        )
                        await self._mark_recording_failed(
                            session, task, "missing_call_metadata"
                        )
                        continue
                    # Attempt to fetch recording
                    recording_url = await self._fetch_recording_url(
                        call_sid, exotel_account_id
                    )

                    if recording_url:
                        s3_key = await self._upload_to_s3(
                            recording_url, str(task.interaction_id)
                        )

                        # Mark as ready
                        task.recording_status = RecordingStatus.READY.value
                        task.recording_s3_key = s3_key
                        task.updated_at = datetime.now()

                        logger.info(
                            "recording_ready",
                            extra={
                                "interaction_id": str(task.interaction_id),
                                "s3_key": s3_key,
                                "poll_attempts": task.recording_retry_count + 1,
                            },
                        )
                        uploaded_count += 1
                    else:
                        # Recording not ready yet, schedule next poll
                        task.recording_retry_count += 1

                        if task.recording_retry_count >= self.MAX_RETRIES:
                            await self._mark_recording_failed(
                                session, task, "max_retries_exceeded"
                            )
                        else:
                            next_delay = self._calculate_backoff(
                                task.recording_retry_count
                            )
                            task.next_poll_at = datetime.now() + timedelta(
                                seconds=next_delay
                            )

                            logger.debug(
                                "recording_not_ready_scheduled_retry",
                                extra={
                                    "interaction_id": str(task.interaction_id),
                                    "attempt": task.recording_retry_count,
                                    "next_poll_in_s": next_delay,
                                },
                            )
                        await session.commit()
                except Exception as e:
                    logger.exception(
                        "recording_poll_error",
                        extra={
                            "interaction_id": str(task.interaction_id),
                            "error": str(e),
                        },
                    )
                    await session.rollback()

                    # Don't mark as failed on transient errors, just let it retry
                    task.recording_retry_count += 1
                    if task.recording_retry_count < self.MAX_RETRIES:
                        next_delay = self._calculate_backoff(task.recording_retry_count)
                        task.next_poll_at = datetime.now() + timedelta(
                            seconds=next_delay
                        )
                        await session.commit()

            logger.info(
                "recording_poll_batch_complete",
                extra={"processed": len(tasks), "uploaded": uploaded_count},
            )

            return uploaded_count

    async def _load_interaction(self, session, interaction_id):
        """Load interaction to get call metadata."""
        from src.models.interaction import Interaction

        stmt = select(Interaction).where(Interaction.id == interaction_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def _fetch_recording_url(
        self,
        call_sid: str,
        account_id: str,
    ) -> Optional[str]:
        """Fetch recording URL from Exotel API."""
        url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("recording_url")
                elif resp.status_code == 404:
                    logger.info(
                        "recording_not_ready",
                        extra={"call_sid": call_sid, "account_id": account_id},
                    )
                    return None
                else:
                    logger.warning(
                        "exotel_unexpected_status",
                        extra={
                            "call_sid": call_sid,
                            "account_id": account_id,
                            "status_code": resp.status_code,
                        },
                    )
                return None
        except httpx.HTTPError as e:
            logger.warning(
                "exotel_http_error",
                extra={"call_sid": call_sid, "account_id": account_id, "error": str(e)},
            )
            return None

    async def _upload_to_s3(self, recording_url: str, interaction_id: str) -> str:
        """Upload recording to S3."""
        s3_key = f"recordings/{interaction_id}.mp3"

        logger.info(
            "recording_upload_start",
            extra={"interaction_id": interaction_id, "s3_key": s3_key},
        )

        # Mock upload for now
        await asyncio.sleep(1)

        logger.info(
            "recording_upload_complete",
            extra={"interaction_id": interaction_id, "s3_key": s3_key},
        )

        return s3_key

    async def _mark_recording_failed(
        self, session, task: PostCallTask, reason: str
    ) -> None:
        """Mark recording as failed."""
        task.recording_status = RecordingStatus.FAILED.value
        task.add_error(f"Recording failed: {reason}")

        logger.error(
            "recording_failed",
            extra={
                "interaction_id": str(task.interaction_id),
                "reason": reason,
                "poll_attempt": task.recording_retry_count,
                "customer_id": str(task.customer_id),
            },
        )

        await self._emit_recording_failure_alert(task)

    async def _emit_recording_failure_alert(self, task: PostCallTask) -> None:
        """Emit recording failure for alerting."""
        # Increment failure counter in Redis for alerting
        alert_key = "alerts:recording_failures:hour"
        await redis_client.incr(alert_key)
        await redis_client.expire(alert_key, 3600)

    def _calculate_backoff(self, attempt: int) -> int:
        """Calculate exponential backoff delay."""
        delay = self.BASE_DELAY_SECONDS * (2 ** (attempt - 1))
        return min(delay, self.MAX_DELAY_SECONDS)


recording_poller = RecordingPoller()
