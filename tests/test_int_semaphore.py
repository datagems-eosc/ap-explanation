"""
A hold of the semaphore lasts for as long as the work under it runs, however
long that is compared with its ttl. Used as a lock (1 unit of 1) and as the
rows budget of a server (many units, shared by the queries of its databases).
"""

import asyncio
import time

import pytest
from testcontainers.community.redis import RedisContainer

from ap_explanation.internal.semaphore import RedisSemaphore


@pytest.fixture(scope="module")
def redis_url():
    with RedisContainer("redis:7-alpine") as redis:
        host = redis.get_container_host_ip()
        port = redis.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


class _Holder:
    """Holds *units* of *name*'s *capacity* until released."""

    def __init__(self, semaphore: RedisSemaphore, name: str, units: int, capacity: int = 10):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.task = asyncio.create_task(self._run(semaphore, name, units, capacity))

    async def _run(self, semaphore: RedisSemaphore, name: str, units: int, capacity: int) -> None:
        async with semaphore.acquire(name, units, capacity):
            self.entered.set()
            await self.release.wait()

    async def done(self) -> None:
        self.release.set()
        await asyncio.wait_for(self.task, 5)


async def _settle() -> None:
    await asyncio.sleep(0.3)


@pytest.mark.asyncio
async def test_lock_outlives_its_ttl(redis_url: str):
    semaphore = RedisSemaphore(redis_url, ttl=0.3, poll=0.05)
    async with semaphore.acquire("lock:long", 1, 1):
        await asyncio.sleep(1)  # three times its ttl
        # Still held: nobody else gets in
        with pytest.raises(TimeoutError):
            async with semaphore.acquire("lock:long", 1, 1, timeout=0.2):
                pass

    # Released at the end; the waiter that gave up stays next for 5 polls
    async with semaphore.acquire("lock:long", 1, 1, timeout=1):
        pass


@pytest.mark.asyncio
async def test_expired_hold_does_not_fail_finished_work(redis_url: str):
    semaphore = RedisSemaphore(redis_url)
    async with semaphore.acquire("lock:expired", 1, 1):
        # Something else removed the hold (it expired while the holder
        # could not reach Redis, for instance)
        semaphore.redis_client.delete("lock:expired")


@pytest.mark.asyncio
async def test_waits_for_room(redis_url: str):
    semaphore = RedisSemaphore(redis_url, poll=0.05)
    a, b = _Holder(semaphore, "budget:room", 6), _Holder(semaphore, "budget:room", 3)
    await asyncio.wait_for(asyncio.gather(a.entered.wait(), b.entered.wait()), 5)  # 9 units of 10: together

    c = _Holder(semaphore, "budget:room", 5)
    other = _Holder(semaphore, "budget:other", 10)
    await asyncio.wait_for(other.entered.wait(), 5)  # a capacity per name
    await _settle()
    assert not c.entered.is_set()

    await a.done()
    await asyncio.wait_for(c.entered.wait(), 5)  # 3 + 5 units
    for holder in (b, c, other):
        await holder.done()


@pytest.mark.asyncio
async def test_large_hold_is_not_starved(redis_url: str):
    semaphore = RedisSemaphore(redis_url, poll=0.05)
    a = _Holder(semaphore, "budget:starve", 6)
    await asyncio.wait_for(a.entered.wait(), 5)
    large = _Holder(semaphore, "budget:starve", 8)
    await _settle()
    # Would fit beside a, but waits behind the large hold
    small = _Holder(semaphore, "budget:starve", 2)
    await _settle()
    assert not large.entered.is_set() and not small.entered.is_set()

    await a.done()
    await asyncio.wait_for(large.entered.wait(), 5)
    await asyncio.wait_for(small.entered.wait(), 5)  # 8 + 2 units
    await large.done()
    await small.done()


@pytest.mark.asyncio
async def test_hold_expires_without_holder(redis_url: str):
    # A worker that crashed holding 10 units: they are free again after its ttl
    semaphore = RedisSemaphore(redis_url, poll=0.05)
    semaphore.redis_client.zadd("budget:crash", {"crashed:10": int(time.time() * 1000) + 300})
    c = _Holder(semaphore, "budget:crash", 10)
    await asyncio.wait_for(c.entered.wait(), 5)
    await c.done()
