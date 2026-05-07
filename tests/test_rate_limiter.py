import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

from src.services.rate_limiter import TokenRateLimiter, RateLimitExceeded
from src.models.customer_config import CustomerConfig
from src.config import settings


@pytest.fixture
def rate_limiter():
    return TokenRateLimiter()


@pytest.fixture
def customer_config():
    return CustomerConfig(
        id="test-config-id",
        customer_id="test-customer-id",
        token_budget_per_minute=10000,
        priority_boost=1.0,
    )


@pytest.fixture
def redis_mock():
    with patch("src.services.rate_limiter.redis_client") as mock:
        # Setup default returns
        mock.get = AsyncMock(return_value="0")
        mock.incrby = AsyncMock()
        mock.expire = AsyncMock()
        mock.pipeline = MagicMock(return_value=MagicMock(execute=AsyncMock()))
        yield mock


@pytest.mark.asyncio
async def test_acquire_budget_success(rate_limiter, customer_config, redis_mock):
    """Test successful budget acquisition."""
    result = await rate_limiter.acquire_budget(
        customer_id="test-customer-id",
        customer_config=customer_config,
        estimated_tokens=1500,
    )

    assert result is True
    # Verify pipeline was called (atomic increment)
    assert redis_mock.pipeline.called


@pytest.mark.asyncio
async def test_acquire_budget_customer_hard_cap(
    rate_limiter, customer_config, redis_mock
):
    """Test rejection when customer hard cap (1.5x) is exceeded."""
    # Simulate customer already used 14000 tokens (hard cap = 15000)
    redis_mock.get = AsyncMock(
        side_effect=lambda key: "14000" if "customer" in key else "0"
    )

    with pytest.raises(RateLimitExceeded) as exc_info:
        await rate_limiter.acquire_budget(
            customer_id="test-customer-id",
            customer_config=customer_config,
            estimated_tokens=1500,
        )

    assert "customer_hard_cap_exceeded" in str(exc_info.value.reason)


@pytest.mark.asyncio
async def test_acquire_budget_global_token_limit(
    rate_limiter, customer_config, redis_mock
):
    """Test rejection when global token limit is exceeded."""
    # Simulate global already at limit
    redis_mock.get = AsyncMock(
        side_effect=lambda key: (
            str(settings.LLM_TOKENS_PER_MINUTE) if "global:tokens" in key else "0"
        )
    )

    with pytest.raises(RateLimitExceeded) as exc_info:
        await rate_limiter.acquire_budget(
            customer_id="test-customer-id",
            customer_config=customer_config,
            estimated_tokens=100,
        )

    assert "global_token_limit_exceeded" in str(exc_info.value.reason)


@pytest.mark.asyncio
async def test_acquire_budget_global_request_limit(
    rate_limiter, customer_config, redis_mock
):
    """Test rejection when global request limit is exceeded."""
    # Simulate global request count at limit
    redis_mock.get = AsyncMock(
        side_effect=lambda key: (
            str(settings.LLM_REQUESTS_PER_MINUTE) if "global:requests" in key else "0"
        )
    )

    with pytest.raises(RateLimitExceeded) as exc_info:
        await rate_limiter.acquire_budget(
            customer_id="test-customer-id",
            customer_config=customer_config,
            estimated_tokens=100,
        )

    assert "global_request_limit_exceeded" in str(exc_info.value.reason)


@pytest.mark.asyncio
async def test_customer_isolation(rate_limiter, redis_mock):
    """Test that Customer A's budget doesn't affect Customer B."""
    config_a = CustomerConfig(
        id="config-a",
        customer_id="customer-a",
        token_budget_per_minute=5000,
        priority_boost=1.0,
    )
    config_b = CustomerConfig(
        id="config-b",
        customer_id="customer-b",
        token_budget_per_minute=5000,
        priority_boost=1.0,
    )

    # Customer A has used 4000 tokens
    def mock_get(key):
        if "customer-a" in key:
            return "4000"
        return "0"

    redis_mock.get = AsyncMock(side_effect=mock_get)

    # Customer A can still use 3500 more (hard cap = 7500)
    result_a = await rate_limiter.acquire_budget(
        customer_id="customer-a",
        customer_config=config_a,
        estimated_tokens=3500,
    )
    assert result_a is True

    # Customer B is unaffected - can use full budget
    result_b = await rate_limiter.acquire_budget(
        customer_id="customer-b",
        customer_config=config_b,
        estimated_tokens=5000,
    )
    assert result_b is True


@pytest.mark.asyncio
async def test_record_actual_usage(rate_limiter, redis_mock):
    """Test adjusting budget when actual tokens differ from estimate."""
    # Just verify it calls pipeline without error
    await rate_limiter.record_actual_usage(
        customer_id="test-customer-id",
        estimated_tokens=1500,
        actual_tokens=1800,
    )

    # Should call pipeline to adjust
    assert redis_mock.pipeline.called


@pytest.mark.asyncio
async def test_get_utilization_percentage(rate_limiter, redis_mock):
    """Test utilization percentage calculation."""
    # Simulate 45,000 tokens used out of 90,000 limit
    redis_mock.get = AsyncMock(return_value="45000")

    utilization = await rate_limiter.get_utilization_percentage()
    assert utilization == 50.0
