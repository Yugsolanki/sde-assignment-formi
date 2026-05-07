import logging
import time
from datetime import datetime
from typing import Optional, Tuple

from src.config import settings
from src.utils.redis_client import redis_client
from src.models.customer_config import CustomerConfig

logger = logging.getLogger(__name__)


class RateLimitExceeded(Exception):
    """Raised when token buget would be exceeded"""

    def __init__(self, reason: str, customer_id: str, tokens: int):
        self.reason = reason
        self.customer_id = customer_id
        self.tokens = tokens
        super().__init__(f"{reason} (customer={customer_id}, tokens={tokens})")


class TokenRateLimiter:
    """
    Manages LLM token rate limits across global and per-customer budgets.

    Uses Redis for fast atomic operations, but state is re-derivable from
    llm_usage_log table on Redis restart.
    """

    def __init__(self):
        self._global_tokens_key = "llm:global:tokens"
        self._global_requests_key = "llm:global:requests"
        self._customer_tokens_prefix = "llm:customer:tokens"
        self._minute_bucket_ttl = 120  # keep 2 min of data for overlap

    def _get_minute_key(self, base_key: str) -> str:
        """Get redis key for current minute bucket"""
        minute_ts = int(time.time() // 60)
        return f"{base_key}:min_{minute_ts}"

    def _get_customer_key(self, customer_id: str) -> str:
        """Get redis key for customer's current minute bucket"""
        base = f"{self._customer_tokens_prefix}:{customer_id}"
        return self._get_minute_key(base)

    async def get_global_usage(self) -> Tuple[int, int]:
        """Get current global token and request usage"""
        tokens_key = self._get_minute_key(self._global_tokens_key)
        requests_key = self._get_minute_key(self._global_requests_key)

        tokens = int(await redis_client.get(tokens_key) or 0)
        requests = int(await redis_client.get(requests_key) or 0)

        return tokens, requests

    async def get_customer_usage(self, customer_id: str) -> int:
        """Get current customer token usage"""
        key = self._get_customer_key(customer_id)
        return int(await redis_client.get(key) or 0)

    async def acquire_budget(
        self, customer_id: str, customer_config: CustomerConfig, estimated_tokens: int
    ) -> bool:
        """
        Attempt to acquire token budget for an LLM call.

        Returns True if budget acquired, False if would exceed limits.
        Raises RateLimitExceeded with specific reason for logging/alerting.
        """
        # Check customer hard cap (1.5x budget)
        customer_used = await self.get_customer_usage(customer_id)
        hard_cap = int(customer_config.token_budget_per_minute * 1.5)

        if customer_used + estimated_tokens > hard_cap:
            logger.warning(
                "customer_hard_cap_exceeded",
                extra={
                    "customer_id": customer_id,
                    "used": customer_used,
                    "requested": estimated_tokens,
                    "hard_cap": hard_cap,
                },
            )
            raise RateLimitExceeded(
                "customer_hard_cap_exceeded", customer_id, estimated_tokens
            )

        # Check global limits
        global_tokens, global_requests = await self.get_global_usage()

        if global_tokens + estimated_tokens > settings.LLM_TOKENS_PER_MINUTE:
            logger.warning(
                "global_token_limit_exceeded",
                extra={
                    "global_used": global_tokens,
                    "requested": estimated_tokens,
                    "limit": settings.LLM_TOKENS_PER_MINUTE,
                },
            )
            raise RateLimitExceeded(
                "global_token_limit_exceeded", customer_id, estimated_tokens
            )

        if global_requests + 1 > settings.LLM_REQUESTS_PER_MINUTE:
            logger.warning(
                "global_request_limit_exceeded",
                extra={
                    "global_requests": global_requests,
                    "limit": settings.LLM_REQUESTS_PER_MINUTE,
                },
            )
            raise RateLimitExceeded(
                "global_request_limit_exceeded", customer_id, estimated_tokens
            )

        # Acquire budget automatically
        pipe = redis_client.pipeline()

        tokens_key = self._get_minute_key(self._global_tokens_key)
        requests_key = self._get_minute_key(self._global_requests_key)
        customer_key = self._get_customer_key(customer_id)

        pipe.incrby(tokens_key, estimated_tokens)
        pipe.expire(tokens_key, self._minute_bucket_ttl)
        pipe.incr(requests_key)
        pipe.expire(requests_key, self._minute_bucket_ttl)
        pipe.incrby(customer_key, estimated_tokens)
        pipe.expire(customer_key, self._minute_bucket_ttl)

        await pipe.execute()

        logger.debug(
            "token_budget_acquired",
            extra={
                "customer_id": customer_id,
                "tokens": estimated_tokens,
                "global_tokens": global_tokens + estimated_tokens,
                "customer_tokens": customer_used + estimated_tokens,
            },
        )

        return True

    async def record_actual_usage(
        self, customer_id: str, estimated_tokens: int, actual_tokens: int
    ) -> None:
        """
        Adjust budget if actual tokens differ from estimate.
        Used after LLM call completes.
        """
        if estimated_tokens == actual_tokens:
            return

        diff = actual_tokens - estimated_tokens
        if diff == 0:
            return

        tokens_key = self._get_minute_key(self._global_tokens_key)
        customer_key = self._get_customer_key(customer_id)

        pipe = redis_client.pipeline()
        pipe.incrby(tokens_key, diff)
        pipe.incrby(customer_key, diff)
        await pipe.execute()

        logger.debug(
            "token_budget_adjusted",
            extra={
                "customer_id": customer_id,
                "estimated": estimated_tokens,
                "actual": actual_tokens,
                "diff": diff,
            },
        )

    async def get_utilization_percentage(self) -> float:
        """Get current global token utilization as percentage"""
        global_tokens, _ = await self.get_global_usage()
        return (global_tokens / settings.LLM_TOKENS_PER_MINUTE) * 100


token_rate_limiter = TokenRateLimiter()
