import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from src.models.base import Base


class CustomerConfig(Base):
    __tablename__ = "customer_configs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    token_budget_per_minute = Column(Integer, nullable=False, default=10000)
    priority_boost = Column(Float, nullable=False, default=1.0)

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def get_effective_budget(self) -> int:
        """Apply priority boos to base budget"""
        return int(self.token_budget_per_minute * self.priority_boost)
