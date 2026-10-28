import asyncio
import logging
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import AbstractContextManager, asynccontextmanager, contextmanager
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


class RedisRowBudget:
    """Rows of aggregates the provenance queries of one PostgreSQL server may build at once.

    ProvSQL builds every aggregate of the server in a single store worker,
    whatever the database: the queries of different databases add up, and
    together crash the server where each alone would not. Each query reserves
    its rows before it runs, and waits until the others leave it enough.

    A reservation is kept alive by its holder and expires after ``ttl``
    seconds without it (a worker that crashed). The first query that does not
    fit becomes the next one: the others wait behind it, so that a large query
    is not starved by a stream of small ones. It stays next for as long as it
    keeps asking, every ``poll`` seconds.
    """

    # KEYS: the reservations ('<ticket>:<rows>', scored by expiry in ms), the next one
    # ARGV: ticket, rows, budget, now (ms), ttl of a reservation (ms), ttl of the next one (ms)
    _RESERVE = """
    redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[4])
    local next = redis.call('GET', KEYS[2])
    if next and next ~= ARGV[1] then return 0 end
    local used = 0
    for _, held in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
      used = used + tonumber(string.match(held, ':(%d+)$'))
    end
    if used + tonumber(ARGV[2]) > tonumber(ARGV[3]) then
      redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[6])
      return 0
    end
    redis.call('ZADD', KEYS[1], tonumber(ARGV[4]) + tonumber(ARGV[5]), ARGV[1] .. ':' .. ARGV[2])
    if next then redis.call('DEL', KEYS[2]) end
    return 1
    """

    def __init__(self, redis_url: str, ttl: float = 60, poll: float = 1):
        self.redis_client = redis.from_url(redis_url)
        self._reserve = self.redis_client.register_script(self._RESERVE)
        self._ttl = ttl
        self._poll = poll

    @asynccontextmanager
    async def reserve(self, server: str, rows: int, budget: int) -> AsyncIterator[None]:
        """Hold *rows* of *server*'s *budget*, waiting until they are free.

        *rows* must not exceed *budget*: such a query would wait forever.
        """
        keys = [f"row_budget:{server}", f"row_budget:{server}:next"]
        ticket = uuid.uuid4().hex
        reservation = f"{ticket}:{rows}"
        ttl_ms = int(self._ttl * 1000)
        waited = time.monotonic()
        while not self._reserve(
                keys, [ticket, rows, budget, int(time.time() * 1000), ttl_ms, int(self._poll * 5000)]):
            await asyncio.sleep(self._poll)
        waited = time.monotonic() - waited
        if waited > self._poll:
            logger.info(f"Waited {waited:.0f} s for {rows:,} rows of '{server}'")

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(self._ttl / 3)
                try:
                    self.redis_client.zadd(keys[0], {reservation: int(time.time() * 1000) + ttl_ms}, xx=True)
                except RedisError as e:
                    # Retried at the next beat, well before the reservation expires
                    logger.warning(f"Could not extend the reservation of '{server}': {e}")

        task = asyncio.create_task(heartbeat())
        try:
            yield
        finally:
            task.cancel()
            try:
                self.redis_client.zrem(keys[0], reservation)
            except RedisError as e:
                # It expires after ttl
                logger.warning(f"Could not release the reservation of '{server}': {e}")
