import asyncio
import logging
import time
import uuid
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol

import redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class Semaphore(Protocol):
    """Named capacities shared by every worker, acquired a number of units at a time."""

    def acquire(
        self, name: str, units: int, capacity: int, timeout: float | None = None
    ) -> AbstractAsyncContextManager[None]:
        """
        Hold *units* of *name*'s *capacity* while the context runs, waiting until they are free.

        *units* must not exceed *capacity*: such a hold would wait forever.

        Raises:
            TimeoutError: If they are not free within *timeout* seconds
        """
        ...


class RedisSemaphore:
    """
    Named capacities shared by every worker through Redis, held a number of
    units at a time.

    A :class:`Semaphore`. Two uses: a per-database lock (1 unit of 1, see ``di.locked``) and the
    rows of aggregates a PostgreSQL server may build at once (see
    ``QueryGuard.reserve``).

    - **Fair**: the first holder that does not fit becomes the next one, and
      the others wait behind it, so that a large hold is not starved by a
      stream of small ones. It stays next for as long as it keeps asking.
    - **Crash-safe**: a hold is extended while its holder runs and expires
      ``ttl`` seconds after it stops (a worker that crashed).
    - **One clock**: expiries are Redis's time, not each worker's.

    The client is synchronous: each Celery task runs its coroutine in a new
    event loop, and an asyncio client's connections belong to the loop that
    opened them. Every call is a single short command.
    """

    # Takes the units if they fit; returns 1 if held, 0 if the caller must ask again.
    _ACQUIRE = """--lua
    -- KEYS[1]: the holds, a sorted set of '<ticket>:<units>' scored by their expiry (ms)
    -- KEYS[2]: the ticket of the next holder, the first one that did not fit
    -- ARGV:   ticket, units, capacity, ttl of a hold (ms), ttl of the next ticket (ms)
    local ticket, units, capacity = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
    local hold_ttl, next_ttl = tonumber(ARGV[4]), ARGV[5]

    -- Redis's clock in ms: the same for every worker
    local time = redis.call('TIME')
    local now = time[1] * 1000 + math.floor(time[2] / 1000)

    -- Holds whose holder stopped extending them (it crashed) give their units back
    redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)

    -- Someone else is next: wait behind it, even if the units would fit
    local next_ticket = redis.call('GET', KEYS[2])
    if next_ticket and next_ticket ~= ticket then return 0 end

    -- The units held: each hold carries its count at the end of its member
    local used = 0
    for _, hold in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
      used = used + tonumber(string.match(hold, ':(%d+)$'))
    end

    if used + units > capacity then
      -- Does not fit: be next, until it stops asking for next_ttl
      redis.call('SET', KEYS[2], ticket, 'PX', next_ttl)
      return 0
    end

    -- ARGV[2] rather than units: Lua would write a large number as 1e+15
    redis.call('ZADD', KEYS[1], now + hold_ttl, ticket .. ':' .. ARGV[2])
    if next_ticket then redis.call('DEL', KEYS[2]) end
    return 1
    """

    # Pushes back the expiry of a hold; returns 0 if it had expired already.
    _EXTEND = """--lua
    -- KEYS[1]: the holds; ARGV: the hold ('<ticket>:<units>'), its ttl (ms)
    if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
    local time = redis.call('TIME')
    local now = time[1] * 1000 + math.floor(time[2] / 1000)
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
    return 1
    """

    def __init__(self, redis_url: str, ttl: float = 60, poll: float = 1):
        """
        Args:
            ttl: Seconds a hold outlives a holder that stopped extending it.
                Extended every ``ttl / 3`` seconds while held.
            poll: Seconds between two attempts while waiting.
        """
        self.redis_client = redis.from_url(
            redis_url)  # pyright: ignore[reportUnknownMemberType]
        self._acquire = self.redis_client.register_script(self._ACQUIRE)
        self._extend = self.redis_client.register_script(self._EXTEND)
        self._ttl_ms = int(ttl * 1000)
        self._poll = poll

    @asynccontextmanager
    async def acquire(
        self, name: str, units: int, capacity: int, timeout: float | None = None
    ) -> AsyncGenerator[None]:
        """See :meth:`Semaphore.acquire`."""
        holds, next_key = name, f"{name}:next"
        ticket = uuid.uuid4().hex
        hold = f"{ticket}:{units}"
        # A waiter that stops asking (it crashed, or gave up) is next no more after 5 polls
        next_ttl_ms = int(self._poll * 5000)

        started = time.monotonic()
        while not self._acquire([holds, next_key], [ticket, units, capacity, self._ttl_ms, next_ttl_ms]):
            if timeout is not None and time.monotonic() - started > timeout:
                raise TimeoutError(
                    f"Waited {timeout:.0f} s for {units:,} units of '{name}'")
            await asyncio.sleep(self._poll)
        waited = time.monotonic() - started
        if waited > self._poll:
            logger.info(
                f"Waited {waited:.0f} s for {units:,} units of '{name}'")

        async def keep_alive() -> None:
            while True:
                await asyncio.sleep(self._ttl_ms / 3000)
                try:
                    if not self._extend([holds], [hold, self._ttl_ms]):
                        logger.error(
                            f"Lost the hold of '{name}' while holding it")
                        return
                except RedisError as e:
                    # Retried at the next beat, well before the hold expires
                    logger.warning(
                        f"Could not extend the hold of '{name}': {e}")

        task = asyncio.create_task(keep_alive())
        try:
            yield
        finally:
            task.cancel()
            try:
                if not self.redis_client.zrem(holds, hold):
                    # The work is done: failing it now would report as
                    # failed something that succeeded
                    logger.warning(
                        f"The hold of '{name}' had expired before its release")
            except RedisError as e:
                logger.warning(
                    f"Could not release the hold of '{name}', it expires after its ttl: {e}")
