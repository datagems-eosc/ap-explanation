import logging
import threading
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Protocol

import redis
from redis.exceptions import LockNotOwnedError, RedisError

logger = logging.getLogger(__name__)


class LockProvider(Protocol):
    """Callable that acquires a named Redis lock and returns it as a context manager.

    Args:
        name: Unique name identifying the lock.
        timeout: Time (seconds) the lock outlives a holder that stopped
            extending it, e.g. a worker that crashed. Extended while held.
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
        timeout: int = 300,
        blocking_timeout: float = 7200,
    ) -> Iterator[None]:
        # thread_local=False: the heartbeat thread extends the lock with the
        # token the acquiring thread got, which a thread-local lock hides.
        lock = self.redis_client.lock(
            name, timeout=timeout, blocking_timeout=blocking_timeout, thread_local=False)
        acquired = lock.acquire(blocking=True)

        if not acquired:
            raise RuntimeError(f"Could not acquire lock '{name}'")

        # The work under the lock has no upper bound (annotating 41 million
        # rows took an hour), so the lock is extended for as long as it runs
        # rather than given a timeout long enough: an expired lock lets a
        # second worker in, and its release then raises.
        stop = threading.Event()

        def heartbeat() -> None:
            while not stop.wait(timeout / 3):
                try:
                    lock.reacquire()
                except LockNotOwnedError:
                    logger.error(f"Lost lock '{name}' while holding it")
                    return
                except RedisError as e:
                    # Retried at the next beat, well before the lock expires
                    logger.warning(f"Could not extend lock '{name}': {e}")

        thread = threading.Thread(
            target=heartbeat, daemon=True, name=f"lock-heartbeat:{name}")
        thread.start()

        try:
            yield
        finally:
            stop.set()
            thread.join()
            try:
                lock.release()
            except LockNotOwnedError:
                # The work is done: failing the task now would report as
                # failed something that succeeded.
                logger.warning(f"Lock '{name}' had expired before its release")
