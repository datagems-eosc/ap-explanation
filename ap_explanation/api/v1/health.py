import os
from typing import Literal

from psycopg import AsyncConnection, OperationalError


async def _check_server(host: str | None, port: str, user: str | None, password: str | None) -> dict:
    """Try to open a connection to the server (using the default 'postgres' database)."""
    if not host:
        return {"status": "unconfigured"}
    try:
        conn = await AsyncConnection.connect(
            f"postgresql://{user}:{password}@{host}:{port}/postgres",
            connect_timeout=5,
        )
        await conn.close()
        return {"status": "reachable"}
    except OperationalError as e:
        return {"status": "unreachable", "detail": str(e)}


async def health_check():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    postgres_status = await _check_server(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=user,
        password=password,
    )
    timescale_status = await _check_server(
        host=os.getenv("POSTGRES_TIMESCALE_HOST"),
        port=os.getenv("POSTGRES_TIMESCALE_PORT", "5433"),
        user=user,
        password=password,
    )

    all_reachable = all(
        s["status"] in ("reachable", "unconfigured")
        for s in (postgres_status, timescale_status)
    )
    overall: Literal["healthy", "degraded"] = "healthy" if all_reachable else "degraded"

    return {
        "status": overall,
        "databases": {
            "postgres": postgres_status,
            "timescale": timescale_status,
        },
    }
