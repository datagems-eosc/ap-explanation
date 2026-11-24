"""
ProvSQL keeps the circuit of every query in its store, and the service none of its
tokens: after a task that grew the store, the service reclaims it, unless another
client is on the database. The service runs as a role that is not a superuser, as in
production, and other applications under roles of their own.
"""

import time
from collections.abc import Callable

import pytest
from psycopg import AsyncConnection

from ap_explanation import di


async def _service_connection(connstr: Callable[..., str]) -> AsyncConnection:
    return await AsyncConnection.connect(connstr(), autocommit=True, application_name=di.APPLICATION_NAME)


async def _gates(connstr: Callable[..., str]) -> int:
    async with await _service_connection(connstr) as conn:
        cursor = await conn.execute("SELECT nb_gates FROM provsql.check_store()")
        return (await cursor.fetchall())[0][0]


async def _run_a_task(connstr: Callable[..., str]) -> int:
    """Grow the store as a task does: a provenance query whose tokens nothing keeps.
    Returns the database's size before, as provenance_service_for reads it. Creates
    the service's role, svc, and another application's, other."""
    async with await _service_connection(connstr) as conn:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS provsql CASCADE")
        await conn.execute("CREATE TABLE IF NOT EXISTS t AS SELECT generate_series(1, 300) AS i")
        await conn.execute("SELECT provsql.add_provenance('t')")
        for role in ("svc", "other"):
            await conn.execute(f"CREATE ROLE {role} LOGIN PASSWORD '{role}'")
        # circuit_cleanup reads the provenance column of every table, ProvSQL's own included
        await conn.execute("GRANT SELECT ON t, provsql.update_provenance TO svc")
        size_before = await di._database_size(conn)
        async with conn.transaction():
            await conn.execute("SET LOCAL provsql.active = 1")
            # 90,000 gates, kept by nothing once the transaction ends
            await conn.execute("CREATE TEMP TABLE r ON COMMIT DROP AS SELECT provsql.provenance() FROM t a, t b")
        return size_before


@pytest.mark.asyncio
async def test_a_task_that_grew_the_store_reclaims_it(connstr: Callable[..., str], monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(di, "CLEANUP_MIN_GROWTH", 1)
    size_before = await _run_a_task(connstr)  # its connection just closed: no other client
    assert await _gates(connstr) > 90_000

    await di.reclaim_circuit_store(connstr("svc", "svc"), size_before)

    assert await _gates(connstr) < 100


@pytest.mark.asyncio
async def test_a_task_that_barely_grew_the_store_leaves_it(connstr: Callable[..., str]):
    size_before = await _run_a_task(connstr)  # a few MB, under CLEANUP_MIN_GROWTH

    await di.reclaim_circuit_store(connstr("svc", "svc"), size_before)

    assert await _gates(connstr) > 90_000


@pytest.mark.asyncio
async def test_another_client_keeps_the_store_without_waiting(
    connstr: Callable[..., str], monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(di, "CLEANUP_MIN_GROWTH", 1)
    size_before = await _run_a_task(connstr)

    # Another application: to the service's role, its session shows no backend_type
    async with await AsyncConnection.connect(connstr("other", "other"), application_name="otherapp"):
        started = time.monotonic()
        await di.reclaim_circuit_store(connstr("svc", "svc"), size_before)
        # circuit_cleanup would hold everyone back for 5 s, then refuse
        assert time.monotonic() - started < 3

    assert await _gates(connstr) > 90_000
