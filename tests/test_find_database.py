"""
find_database builds the connection string from a database name the request
carries, so the name must not be able to change where it connects.
"""

import pytest
from psycopg.conninfo import conninfo_to_dict

from ap_explanation import di


@pytest.mark.asyncio
async def test_db_name_cannot_redirect_the_connection(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    monkeypatch.delenv("POSTGRES_TIMESCALE_HOST", raising=False)

    class Conn:
        async def close(self):
            pass

    async def connect(conninfo):
        connect.seen = conninfo
        return Conn()

    monkeypatch.setattr(di.AsyncConnection, "connect", connect)

    await di.find_database("mathe?host=evil.example")

    params = conninfo_to_dict(connect.seen)
    assert params["host"] == "db.internal"
    assert params["dbname"] == "mathe?host=evil.example"
