"""
The per-database lock holds for as long as the work under it runs, however
long that is compared with its timeout.
"""

import time

import pytest
from testcontainers.community.redis import RedisContainer

from ap_explanation.internal.distributed_lock import RedisLockProvider


@pytest.fixture(scope="module")
def lock_provider():
    with RedisContainer("redis:7-alpine") as redis:
        host = redis.get_container_host_ip()
        port = redis.get_exposed_port(6379)
        yield RedisLockProvider(f"redis://{host}:{port}/0")


def test_lock_outlives_its_timeout(lock_provider: RedisLockProvider):
    with lock_provider.acquire("test_lock:long", timeout=1):
        time.sleep(3)
        # Still held: nobody else gets in
        with pytest.raises(RuntimeError):
            with lock_provider.acquire("test_lock:long", timeout=1, blocking_timeout=0.1):
                pass

    # Released at the end
    with lock_provider.acquire("test_lock:long", timeout=1, blocking_timeout=0.1):
        pass


def test_expired_lock_does_not_fail_finished_work(lock_provider: RedisLockProvider):
    with lock_provider.acquire("test_lock:expired", timeout=5):
        # Something else removed the lock (it expired while the heartbeat
        # could not reach Redis, for instance)
        lock_provider.redis_client.delete("test_lock:expired")
