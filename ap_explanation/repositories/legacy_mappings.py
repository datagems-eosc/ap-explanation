"""
What earlier versions left in annotated databases: a mapping table per semiring,
``<table>_prov<semiring>``, and a relation per semiring unioning them,
``<semiring>_mapping``.

Tables are migrated when annotated again. This module, and what calls it, can go
once every database has been.
"""

from logging import getLogger

from psycopg import AsyncConnection
from psycopg.sql import SQL, Identifier

from ap_explanation.semirings import semirings

logger = getLogger(__name__)

LEGACY_SUFFIXES = [f"_prov{s.name}" for s in semirings]


def legacy_tables_of(table_name: str) -> list[str]:
    """The names earlier versions gave the mapping tables of *table_name*."""
    return [f"{table_name}{suffix}" for suffix in LEGACY_SUFFIXES]


async def replace_legacy_tables(
    conn: AsyncConnection, legacy_tables: list[str], prov_table: str, keep: str | None
) -> None:
    """
    Rename *keep*, if any, to the shared *prov_table*, and drop the other
    per-semiring tables, identical copies of it or outdated ones.
    """
    async with conn.transaction():
        for table in legacy_tables:
            if table == keep:
                # The registry keys on the mapping's oid, which a rename
                # keeps: the mapping stays maintained.
                logger.info(f"Migrating provenance table '{table}' to '{prov_table}'")
                await conn.execute(
                    SQL("ALTER TABLE {} RENAME TO {}").format(Identifier(table), Identifier(prov_table))
                )
            else:
                logger.info(f"Dropping per-semiring provenance table '{table}'")
                await conn.execute(SQL("DROP TABLE {} CASCADE").format(Identifier(table)))


async def drop_union_relations(conn: AsyncConnection, schema_name: str) -> None:
    """
    Drop the per-semiring unions of a schema. A database annotated by an old
    enough version has materialised snapshots rather than views, and DROP VIEW
    IF EXISTS raises WrongObjectType on those: IF EXISTS only guards against
    the name being absent, so the statement follows what is actually there.
    """
    cursor = await conn.execute(
        "SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = %s AND c.relname = ANY(%s)",
        (schema_name, [f"{s.name}_mapping" for s in semirings]),
    )
    for name, kind in await cursor.fetchall():
        await conn.execute(
            SQL("DROP {} {}.{} CASCADE").format(
                SQL("VIEW" if kind == "v" else "TABLE"), Identifier(schema_name), Identifier(name)
            )
        )
