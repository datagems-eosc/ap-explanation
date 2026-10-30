"""
find_database builds the connection string from a database name the request
carries, so the name must not be able to change where it connects.
"""

import pytest
from psycopg.conninfo import conninfo_to_dict

from ap_explanation import di
from ap_explanation.settings import Settings


@pytest.mark.asyncio
async def test_db_name_cannot_redirect_the_connection(monkeypatch):
    monkeypatch.setattr(di, "settings", Settings(
        postgres_user="u",
        postgres_password="p",
        postgres_host="db.internal",
        postgres_timescale_host=None,
    ))

    class Conn:
        async def close(self):
            pass

    seen = []

    async def connect(conninfo):
        seen.append(conninfo)
        return Conn()

    monkeypatch.setattr(di.AsyncConnection, "connect", connect)

    await di.find_database("mathe?host=evil.example")

    params = conninfo_to_dict(seen[0])
    assert params["host"] == "db.internal"
    assert params["dbname"] == "mathe?host=evil.example"
