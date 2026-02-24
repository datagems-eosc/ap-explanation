from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Protocol

import redis


class LockProvider(Protocol):
    """Callable that acquires a named Redis lock and returns it as a context manager.

    Args:
        name: Unique name identifying the lock.
        timeout: Maximum time (seconds) the lock is held before auto-release.
        blocking_timeout: Maximum time (seconds) to wait when acquiring the lock.
    """

    def acquire(
        self,
        name: str,
        timeout: int = ...,
        blocking_timeout: float = ...,
    ) -> AbstractContextManager[None]: ...


class RedisLockProvider:
    """Implementation of LockProvider using Redis locks."""

    def __init__(self, redis_url: str):
        self.redis_client = redis.from_url(redis_url)

    @contextmanager
    def acquire(
        self,
        name: str,
        timeout: int = 3600,
        blocking_timeout: float = 7200,
    ) -> Iterator[None]:
        lock = self.redis_client.lock(
            name, timeout=timeout, blocking_timeout=blocking_timeout)
        acquired = lock.acquire(blocking=True)

        if not acquired:
            raise RuntimeError(f"Could not acquire lock '{name}'")

        try:
            yield
        finally:
            lock.release()
