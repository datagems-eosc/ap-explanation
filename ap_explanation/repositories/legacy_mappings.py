"""
What earlier versions left in annotated databases: a ``provsql_ref`` column on
each table, mapping tables ``<table>_provkey`` or ``<table>_prov<semiring>``,
and a relation unioning them, ``provkey_mapping`` or ``<semiring>_mapping``.

Nothing reads them anymore. They are removed when a table is annotated again.
This module, and what calls it, can go once every database has been.
"""

from logging import getLogger

from psycopg import AsyncConnection
from psycopg.sql import SQL, Identifier

from ap_explanation.domains.provenance.semiring import semirings

logger = getLogger(__name__)

LEGACY_REFERENCE_COLUMN = "provsql_ref"
LEGACY_UNIONS = ["provkey_mapping", *(f"{s.name}_mapping" for s in semirings)]


def legacy_tables_of(table_name: str) -> list[str]:
    """The names earlier versions gave the mapping tables of *table_name*."""
    return [f"{table_name}_provkey", *(f"{table_name}_prov{s.name}" for s in semirings)]


async def remove_legacy_mappings(conn: AsyncConnection, schema_name: str, table_name: str) -> None:
    """
    Remove what earlier versions left for a table, and their unions of its schema.

    Only relations with a provenance column, as all of these have: a dataset may
    use one of the names for a table of its own.

    A database annotated by an old enough version has materialised snapshots
    rather than views, and DROP VIEW IF EXISTS raises WrongObjectType on those:
    IF EXISTS only guards against the name being absent, so the statement
    follows what is actually there.
    """
    cursor = await conn.execute(
        "SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
        " JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'provenance' AND NOT a.attisdropped"
        " WHERE n.nspname = %s AND c.relname = ANY(%s)"
        " ORDER BY c.relkind = 'v' DESC",  # the views first, as they read the tables
        (schema_name, [*LEGACY_UNIONS, *legacy_tables_of(table_name)]),
    )
    relations = await cursor.fetchall()
    cursor = await conn.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns"
        " WHERE table_schema = %s AND table_name = %s AND column_name = %s)",
        (schema_name, table_name, LEGACY_REFERENCE_COLUMN),
    )
    has_reference_column = (await cursor.fetchone())[0]

    if not relations and not has_reference_column:
        return

    logger.info(f"Removing what earlier versions left for '{schema_name}.{table_name}'")
    async with conn.transaction():
        for name, kind in relations:
            await conn.execute(
                SQL("DROP {} {}.{} CASCADE").format(
                    SQL("VIEW" if kind == "v" else "TABLE"), Identifier(schema_name), Identifier(name)
                )
            )
        if has_reference_column:
            await conn.execute(
                SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(
                    Identifier(schema_name), Identifier(table_name), Identifier(LEGACY_REFERENCE_COLUMN)
                )
            )
