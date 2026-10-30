"""
The per-database lock holds for as long as the work under it runs, however
long that is compared with its timeout. The rows budget of a server is shared
by the queries of all its databases.
"""

import asyncio
import time

import pytest
from testcontainers.community.redis import RedisContainer

from ap_explanation.internal.distributed_lock import RedisLockProvider, RedisRowBudget


@pytest.fixture(scope="module")
def redis_url():
    with RedisContainer("redis:7-alpine") as redis:
        host = redis.get_container_host_ip()
        port = redis.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest.fixture(scope="module")
def lock_provider(redis_url: str):
    return RedisLockProvider(redis_url)


def test_lock_outlives_its_timeout(lock_provider: RedisLockProvider):
    with lock_provider.acquire("test_lock:long", timeout=1):
        time.sleep(3)
        # Still held: nobody else gets in
        with pytest.raises(RuntimeError), lock_provider.acquire("test_lock:long", timeout=1, blocking_timeout=0.1):
            pass

    # Released at the end
    with lock_provider.acquire("test_lock:long", timeout=1, blocking_timeout=0.1):
        pass


def test_expired_lock_does_not_fail_finished_work(lock_provider: RedisLockProvider):
    with lock_provider.acquire("test_lock:expired", timeout=5):
        # Something else removed the lock (it expired while the heartbeat
        # could not reach Redis, for instance)
        lock_provider.redis_client.delete("test_lock:expired")


class _Query:
    """A query holding *rows* of *server*'s budget of *total* until released."""

    def __init__(self, budget: RedisRowBudget, server: str, rows: int, total: int = 10):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.task = asyncio.create_task(self._run(budget, server, rows, total))

    async def _run(self, budget: RedisRowBudget, server: str, rows: int, total: int) -> None:
        async with budget.reserve(server, rows, total):
            self.entered.set()
            await self.release.wait()

    async def done(self) -> None:
        self.release.set()
        await asyncio.wait_for(self.task, 5)


async def _settle() -> None:
    await asyncio.sleep(0.3)


@pytest.mark.asyncio
async def test_budget_waits_for_room(redis_url: str):
    budget = RedisRowBudget(redis_url, poll=0.05)
    a, b = _Query(budget, "budget:room", 6), _Query(budget, "budget:room", 3)
    await asyncio.wait_for(asyncio.gather(a.entered.wait(), b.entered.wait()), 5)  # 9 rows of 10: together

    c = _Query(budget, "budget:room", 5)
    other_server = _Query(budget, "budget:other", 10)
    await asyncio.wait_for(other_server.entered.wait(), 5)  # a budget per server
    await _settle()
    assert not c.entered.is_set()

    await a.done()
    await asyncio.wait_for(c.entered.wait(), 5)  # 3 + 5 rows
    for q in (b, c, other_server):
        await q.done()


@pytest.mark.asyncio
async def test_budget_large_query_is_not_starved(redis_url: str):
    budget = RedisRowBudget(redis_url, poll=0.05)
    a = _Query(budget, "budget:starve", 6)
    await asyncio.wait_for(a.entered.wait(), 5)
    large = _Query(budget, "budget:starve", 8)
    await _settle()
    # Would fit beside a, but waits behind the large query
    small = _Query(budget, "budget:starve", 2)
    await _settle()
    assert not large.entered.is_set() and not small.entered.is_set()

    await a.done()
    await asyncio.wait_for(large.entered.wait(), 5)
    await asyncio.wait_for(small.entered.wait(), 5)  # 8 + 2 rows
    await large.done()
    await small.done()


@pytest.mark.asyncio
async def test_budget_outlives_its_ttl_and_expires_without_holder(redis_url: str):
    budget = RedisRowBudget(redis_url, ttl=0.3, poll=0.05)
    a = _Query(budget, "budget:ttl", 10)
    await asyncio.wait_for(a.entered.wait(), 5)
    await asyncio.sleep(1)  # three times its ttl: kept alive while held
    b = _Query(budget, "budget:ttl", 1)
    await _settle()
    assert not b.entered.is_set()
    await a.done()
    await asyncio.wait_for(b.entered.wait(), 5)
    await b.done()

    # A worker that crashed holding 10 rows: they are free again after its ttl
    budget.redis_client.zadd("row_budget:budget:crash", {"crashed:10": int(time.time() * 1000) + 300})
    c = _Query(budget, "budget:crash", 10)
    await asyncio.wait_for(c.entered.wait(), 5)
    await c.done()
