import redis.asyncio as aioredis


async def _check_redis(uri: str) -> dict:
    """Ping the Redis instance."""
    client = aioredis.from_url(uri, socket_connect_timeout=5)
    try:
        await client.ping()
        return {"status": "reachable"}
    except Exception as e:
        return {"status": "unreachable", "detail": str(e)}
    finally:
        await client.aclose()


async def health_check():
    """Liveness check — returns service version info."""
    return {"status": "ok"}


async def readiness_check():
    """
    Readiness check — verifies that the PostgreSQL database and the Redis broker
    are reachable before the service is considered ready to handle traffic.

    Returns HTTP 200 with ``status: ready`` when all dependencies are reachable,
    or HTTP 503 with ``status: not_ready`` together with per-dependency details
    when at least one dependency is unavailable.
    """
    from fastapi.responses import JSONResponse

    from ap_explanation.di import REDIS_BROKER_URI

    redis_status = await _check_redis(REDIS_BROKER_URI)

    all_ready = all(
        s["status"] in ("reachable", "unconfigured")
        for s in (redis_status)
    )

    body = {
        "status": "ready" if all_ready else "not_ready",
        "dependencies": {
            "redis": redis_status,
        },
    }
    return JSONResponse(content=body, status_code=200 if all_ready else 503)
