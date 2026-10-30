"""
Integration tests for the superuser / extension-presence guard in
AnnotationRepository.enable(), using the postgres_container fixture
from conftest (each test gets its own isolated container).

Cases covered:
  1. provsql already installed + superuser  → succeeds, no rolsuper check
  2. provsql NOT installed + non-superuser  → raises ProvSqlMissingError
  3. provsql NOT installed + superuser      → installs extension, succeeds
"""

import pytest
from psycopg import AsyncConnection

from ap_explanation.errors import ProvSqlMissingError
from ap_explanation.repositories.annotation import AnnotationRepository
from ap_explanation.repositories.provsql import activate_provsql


@pytest.mark.asyncio
async def test_enable_provenance_already_installed_succeeds(connstr):
    """
    When provsql is already installed and the user is a superuser,
    enable must succeed without performing the rolsuper check.
    """
    async with await AsyncConnection.connect(connstr(), autocommit=True) as conn:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS provsql CASCADE")
        await conn.execute("DROP TABLE IF EXISTS public.test_ep_installed CASCADE")
        await conn.execute("CREATE TABLE public.test_ep_installed (id serial PRIMARY KEY, val text)")

        await activate_provsql(conn)
        repo = AnnotationRepository(conn)
        result = await repo.enable("public", "test_ep_installed")

    assert result is True


@pytest.mark.asyncio
async def test_enable_provenance_not_installed_non_superuser_raises(connstr):
    """
    When provsql is not installed and the connecting user is not a superuser,
    enable must raise ProvSqlMissingError without attempting
    CREATE EXTENSION.
    """
    # Set up: drop provsql and create a limited login role
    async with await AsyncConnection.connect(connstr(), autocommit=True) as admin:
        await admin.execute("DROP EXTENSION IF EXISTS provsql CASCADE")
        await admin.execute("DROP ROLE IF EXISTS limited_ep_user")
        await admin.execute("CREATE ROLE limited_ep_user WITH LOGIN PASSWORD 'limited'")
        await admin.execute("GRANT CONNECT ON DATABASE mathe TO limited_ep_user")
        await admin.execute("GRANT USAGE ON SCHEMA public TO limited_ep_user")

    async with await AsyncConnection.connect(
        connstr("limited_ep_user", "limited"), autocommit=True
    ) as conn:
        repo = AnnotationRepository(conn)
        with pytest.raises(ProvSqlMissingError) as exc_info:
            await repo.enable("public", "test_ep_noperm")

    assert "not a superuser" in str(exc_info.value)


@pytest.mark.asyncio
async def test_enable_provenance_not_installed_superuser_installs_extension(connstr):
    """
    When provsql is not installed but the user is a superuser,
    enable must install the extension and succeed.

    """
    async with await AsyncConnection.connect(connstr(), autocommit=True) as conn:
        await conn.execute("DROP EXTENSION IF EXISTS provsql CASCADE")
        await conn.execute("DROP TABLE IF EXISTS public.test_ep_superuser CASCADE")
        await conn.execute("CREATE TABLE public.test_ep_superuser (id serial PRIMARY KEY, val text)")

        await activate_provsql(conn)
        repo = AnnotationRepository(conn)
        result = await repo.enable("public", "test_ep_superuser")

        assert result is True

        # Verify provsql was installed by enable
        cursor = await conn.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'provsql')"
        )
        assert (await cursor.fetchone())[0] is True
