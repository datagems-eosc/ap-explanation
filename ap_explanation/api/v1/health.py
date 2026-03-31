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
    from fastapi.responses import JSONResponse

    from ap_explanation.di import REDIS_BROKER_URI

    redis_status = await _check_redis(REDIS_BROKER_URI)

    # Put all dependency dicts in a list
    dependencies = [redis_status]

    all_ready = all(s["status"] in ("reachable", "unconfigured")
                    for s in dependencies)

    body = {
        "status": "ready" if all_ready else "not_ready",
        "dependencies": {
            "redis": redis_status,
        },
    }
    return JSONResponse(content=body, status_code=200 if all_ready else 503)
