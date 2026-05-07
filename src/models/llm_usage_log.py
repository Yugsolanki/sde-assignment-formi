import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID

from src.models.base import Base


class LLMUsageLog(Base):
    __tablename__ = "llm_usage_log"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    interaction_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    customer_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    campaign_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    tokens_used = Column(Integer, nullable=False)
    latency_ms = Column(Integer, nullable=False)

    call_stage = Column(String(50), nullable=True)
    model = Column(String(100), nullable=False)
    provider = Column(String(50), nullable=False)

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )
