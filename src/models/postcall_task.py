import enum
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    SmallInteger,
    String,
    func,
    Enum,
)
from sqlalchemy.dialects.postgresql import UUID, JSON as PG_JSONB

from src.models.base import Base


class TaskStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEFERRED = "DEFERRED"


class RecordingStatus(str, enum.Enum):
    PENDING = "PENDING"
    READY = "READY"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class PriorityClass(int, enum.Enum):
    HIGH = 0  # P0: Interested, confirmed booking
    NORMAL = 1  # P1: Standard calls
    LOW = 2  # P2: Not interested, voicemail


class PostCallTask(Base):
    __tablename__ = "postcall_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    interaction_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    priority_class = Column(
        SmallInteger, nullable=False, default=PriorityClass.NORMAL.value, index=True
    )
    customer_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    status = Column(
        "status",
        Enum(TaskStatus, name="task_status"),
        nullable=False,
        default=TaskStatus.QUEUED,
        index=True,
    )
    scheduled_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    recording_status = Column(
        "recording_status",
        Enum(RecordingStatus, name="recording_status"),
        nullable=False,
        default=RecordingStatus.PENDING,
    )
    recording_s3_key = Column(String(512), nullable=True)
    recording_retry_count = Column(Integer, nullable=False, default=0)
    next_poll_at = Column(DateTime(timezone=True), nullable=False, default=func.now())

    estimated_tokens = Column(Integer, nullable=False)
    actual_tokens = Column(Integer, nullable=True)

    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=5)
    error_log = Column(PG_JSONB, nullable=False, default=list)

    downstream_triggers = Column(PG_JSONB, nullable=False, default=dict)

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    version = Column(Integer, nullable=False, default=1)

    @property
    def is_ready_for_processing(self) -> bool:
        """Check if task can be picked up by scheduler."""
        if self.status not in (TaskStatus.QUEUED, TaskStatus.DEFERRED):
            return False
        if self.scheduled_at > datetime.now(timezone.utc)():
            return False
        return True

    def add_error(self, error: str) -> None:
        """Append error to error_log."""
        errors = self.error_log or []
        errors.append(
            {
                "error": error,
                "timestamp": datetime.now(timezone.utc)().isoformat(),
            }
        )
        self.error_log = errors

    def get_duration_seconds(self) -> Optional[float]:
        """Calculate total processing duration."""
        if not self.started_at or not self.completed_at:
            return None
        return (self.completed_at - self.started_at).total_seconds()
