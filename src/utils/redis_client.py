import os

import redis.asyncio as aioredis

from src.config import settings


class _LazyRedis:
    """
    Lazy Redis client that creates the connection pool on first use.
    This is necessary to avoid sharing a connection pool across forked processes, which can lead to unexpected behavior and connection issues.  By checking the process ID, we ensure that each process gets its own Redis client instance with a separate connection pool.  The `reset()` method allows us to clear the inherited state in a child process, ensuring that a new client is created on the next access.  This is particularly important in a Celery worker context, where multiple worker processes are spawned using fork.  By connecting to the `worker_process_init` signal, we ensure that the Redis client is reset in each worker process, preventing any issues with shared connections.
    """

    _client: aioredis.Redis | None = None
    _pid: int = -1

    def _get(self) -> aioredis.Redis:
        """Get the Redis client, creating it if necessary."""
        pid = os.getpid()
        if self._client is None or self._pid != pid:
            self._client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
            self._pid = pid
        return self._client

    def __getattr__(self, name):
        return getattr(self._get(), name)

    def reset(self) -> None:
        """Close inherited state so a fresh client is created on next access."""
        self._client = None
        self._pid = -1


redis_client: _LazyRedis = _LazyRedis()


async def get_redis() -> aioredis.Redis:
    return redis_client  # type: ignore[return-value]


# ── Celery integration ────────────────────────────────────────────────────────

# To ensure that each Celery worker process has its own Redis client instance, we connect to the `worker_process_init` signal. This signal is emitted when a worker process is initialized, allowing us to reset the Redis client and ensure that a new connection pool is created for each worker process.
from celery.signals import worker_process_init  # noqa: E402


@worker_process_init.connect
def reset_redis_on_fork(**kwargs):
    redis_client.reset()
