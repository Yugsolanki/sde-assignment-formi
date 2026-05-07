import pytest
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock

from src.services.rate_limiter import token_rate_limiter, RateLimitExceeded
from src.services.task_scheduler import TaskScheduler
from src.models.postcall_task import PostCallTask, TaskStatus, PriorityClass
from src.models.customer_config import CustomerConfig
from src.config import settings


@pytest.mark.asyncio
async def test_no_429_under_rate_limit():
    """
    Test AC1: System never fires LLM requests beyond configured rate limits.

    Simulate burst of 1000 calls and verify rate limit is respected.
    """
    scheduler = TaskScheduler()

    # Create mock tasks
    tasks = []
    for i in range(1000):
        task = PostCallTask(
            id=f"task-{i}",
            interaction_id=f"interaction-{i}",
            customer_id="customer-1",
            priority_class=PriorityClass.NORMAL.value,
            estimated_tokens=1500,
            status=TaskStatus.QUEUED.value,
            scheduled_at=datetime.utcnow(),
        )
        tasks.append(task)

    # Mock database
    with (
        patch("src.services.task_scheduler.async_session_factory") as session_mock,
        patch("src.services.rate_limiter.redis_client") as redis_mock,
    ):

        session = AsyncMock()
        session_factory = AsyncMock()
        session_factory.return_value.__aenter__ = AsyncMock(return_value=session)
        session_factory.return_value.__aexit__ = AsyncMock()
        session_mock.return_value = session_factory

        # Track token usage
        global_tokens = 0
        redis_calls = []

        def mock_incrby(key, value):
            nonlocal global_tokens
            if "global:tokens" in key:
                global_tokens += value
                redis_calls.append(("incrby", key, value))

        class FakePipeline:
            def __init__(self):
                self.calls = []
            def incrby(self, key, value):
                self.calls.append(("incrby", key, value))
                return self
            def incr(self, key):
                self.calls.append(("incrby", key, 1))
                return self
            def expire(self, key, ttl):
                return self
            async def execute(self):
                for cmd, key, val in self.calls:
                    mock_incrby(key, val)
                self.calls = []

        def mock_get(key):
            if "global:tokens" in key:
                return str(global_tokens)
            if "global:requests" in key:
                return "0"
            if "customer" in key:
                return "0"
            return "0"

        redis_mock.get = AsyncMock(side_effect=mock_get)
        redis_mock.incrby = AsyncMock(side_effect=mock_incrby)
        redis_mock.expire = AsyncMock()
        redis_mock.pipeline = MagicMock(return_value=FakePipeline())

        # Configure customer
        customer_config = CustomerConfig(
            customer_id="customer-1",
            token_budget_per_minute=50000,
            priority_boost=1.0,
        )

        with patch.object(
            scheduler, "_get_customer_config", return_value=customer_config
        ):
            # Process tasks
            started = 0
            for task in tasks:
                try:
                    success = await scheduler._process_task(task)
                    if success:
                        started += 1
                except RateLimitExceeded:
                    pass  # Expected when limit hit

        # Verify we never exceeded global limit
        assert global_tokens <= settings.LLM_TOKENS_PER_MINUTE

        # Verify we processed some tasks (not all due to limit)
        assert started > 0
        assert started < 1000  # Can't process all due to limit

        print(f"Processed {started} out of 1000 tasks without exceeding rate limit")
        print(f"Final token usage: {global_tokens}/{settings.LLM_TOKENS_PER_MINUTE}")


@pytest.mark.asyncio
async def test_customer_budget_isolation():
    """
    Test AC2: Per-customer token budget enforced.
    Customer A's budget does not consume Customer B's allocation.
    """
    scheduler = TaskScheduler()

    # Create tasks for two customers
    tasks_a = []
    tasks_b = []

    # Customer A: 20 tasks at 1500 tokens each = 30,000 tokens
    for i in range(20):
        tasks_a.append(
            PostCallTask(
                id=f"task-a-{i}",
                interaction_id=f"interaction-a-{i}",
                customer_id="customer-a",
                priority_class=PriorityClass.HIGH.value,
                estimated_tokens=1500,
                status=TaskStatus.QUEUED.value,
                scheduled_at=datetime.utcnow(),
            )
        )

    # Customer B: 5 tasks at 1500 tokens each = 7,500 tokens
    for i in range(5):
        tasks_b.append(
            PostCallTask(
                id=f"task-b-{i}",
                interaction_id=f"interaction-b-{i}",
                customer_id="customer-b",
                priority_class=PriorityClass.HIGH.value,
                estimated_tokens=1500,
                status=TaskStatus.QUEUED.value,
                scheduled_at=datetime.utcnow(),
            )
        )

    # Mock database
    with (
        patch("src.services.task_scheduler.async_session_factory") as session_mock,
        patch("src.services.rate_limiter.redis_client") as redis_mock,
    ):

        session = AsyncMock()
        session_factory = AsyncMock()
        session_factory.return_value.__aenter__ = AsyncMock(return_value=session)
        session_factory.return_value.__aexit__ = AsyncMock()
        session_mock.return_value = session_factory

        # Track per-customer usage
        customer_usage = {"customer-a": 0, "customer-b": 0}
        global_usage = 0

        def mock_get(key):
            if "customer-a" in key:
                return str(customer_usage["customer-a"])
            elif "customer-b" in key:
                return str(customer_usage["customer-b"])
            elif "global:tokens" in key:
                return str(global_usage)
            return "0"

        def mock_incrby(key, value):
            nonlocal global_usage
            if "customer-a" in key:
                customer_usage["customer-a"] += value
            elif "customer-b" in key:
                customer_usage["customer-b"] += value
            elif "global:tokens" in key:
                global_usage += value

        class FakePipeline:
            def __init__(self):
                self.calls = []
            def incrby(self, key, value):
                self.calls.append(("incrby", key, value))
                return self
            def incr(self, key):
                self.calls.append(("incrby", key, 1))
                return self
            def expire(self, key, ttl):
                return self
            async def execute(self):
                for cmd, key, val in self.calls:
                    mock_incrby(key, val)
                self.calls = []

        redis_mock.get = AsyncMock(side_effect=mock_get)
        redis_mock.incrby = AsyncMock(side_effect=mock_incrby)
        redis_mock.expire = AsyncMock()
        redis_mock.pipeline = MagicMock(return_value=FakePipeline())

        # Customer A has small budget (10,000 tokens)
        # Customer B has larger budget (50,000 tokens)
        def get_config(customer_id):
            if customer_id == "customer-a":
                return CustomerConfig(
                    customer_id="customer-a",
                    token_budget_per_minute=10000,
                    priority_boost=1.0,
                )
            else:
                return CustomerConfig(
                    customer_id="customer-b",
                    token_budget_per_minute=50000,
                    priority_boost=1.0,
                )

        # First, exhaust Customer A's budget
        started_a = 0
        for task in tasks_a:
            with patch.object(
                scheduler,
                "_get_customer_config",
                return_value=get_config(task.customer_id),
            ):
                try:
                    success = await scheduler._process_task(task)
                    if success:
                        started_a += 1
                except RateLimitExceeded:
                    pass

        # Customer A should hit hard cap (15,000 = 10,000 * 1.5)
        assert customer_usage["customer-a"] <= 15000

        # Now try Customer B - should still work
        started_b = 0
        for task in tasks_b:
            with patch.object(
                scheduler,
                "_get_customer_config",
                return_value=get_config(task.customer_id),
            ):
                try:
                    success = await scheduler._process_task(task)
                    if success:
                        started_b += 1
                except RateLimitExceeded:
                    pass

        # Customer B should process all tasks (7,500 tokens < 50,000 budget)
        assert started_b == 5
        assert customer_usage["customer-b"] == 7500

        print(
            f"Customer A: started {started_a}/20 tasks, used {customer_usage['customer-a']} tokens"
        )
        print(
            f"Customer B: started {started_b}/5 tasks, used {customer_usage['customer-b']} tokens"
        )


@pytest.mark.asyncio
async def test_short_transcript_no_llm():
    """
    Test AC8: Short transcripts (< 4 turns) never consume LLM quota.
    """
    from src.services.priority_classifier import priority_classifier

    short_transcript = "Agent: Hello.\nCustomer: Not interested."

    priority = priority_classifier.classify(short_transcript)

    # Short transcript should be LOW priority (fast path)
    assert priority == PriorityClass.LOW

    # Even if processed, token estimation should be minimal
    tokens = priority_classifier.estimate_tokens(short_transcript)
    assert tokens == 500  # Minimum, no actual transcript processing
