from collections import defaultdict
from logging import getLogger
from typing import Any, LiteralString, cast

from orjson import dumps
from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg.types.json import set_json_dumps

from provenance_demo.internal.sql_rewriter import SqlRewriter
from provenance_demo.types.semiring import DbSemiring

logger = getLogger(__name__)
set_json_dumps(dumps)


class ProvenanceRepository:
    """
    Repository for all provenance-related operations.

    Handles both provenance setup (annotations) and querying with provenance tracking.
    """
    _conn: AsyncConnection
    _sql_rewriter: SqlRewriter
    _semiring: DbSemiring

    def __init__(
        self,
        conn: AsyncConnection,
        semiring: DbSemiring,
        sql_rewriter: SqlRewriter
    ):
        self._conn = conn
        self._semiring = semiring
        self._sql_rewriter = sql_rewriter

    async def query(self, schema_name: str, query: str, semiring: DbSemiring) -> list[dict[str, Any]]:
        """
        Execute a SQL query and return the results as a list of dictionaries.
        :param query: The SQL query to execute.
        :return: List of result rows as dictionaries.
        """
        edited_query = self._sql_rewriter.rewrite(query, semiring)

        async with self._conn.transaction():
            await self._set_search_path(schema_name)

            # Fetch the provenance-annotated results
            cursor = await self._conn.cursor(row_factory=dict_row).execute(SQL(cast(LiteralString, edited_query)))
            rows = await cursor.fetchall()

            # From each row, retrieve the provenance data
            for row in rows:
                row[semiring.name] = await self._fetch_related_data(row[semiring.retrieval_function], semiring)

        return rows

    async def enable_provenance_for(self, schema_name: str, table_name: str) -> None:
        """
        Create the provenance annotations for a given base table in the specified schema.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.
        """
        prov_table = self._semiring.get_provenance_table_name_for(table_name)

        await self._set_search_path(schema_name)

        # Add the provenance column to the base table (separate transaction)
        try:
            async with self._conn.transaction():
                await self._conn.execute("SELECT add_provenance(%s)", (table_name,))
        except errors.DuplicateColumn:
            logger.info(
                f"Provenance column for table '{table_name}' already exists, ignoring")

        # Create the provenance mapping table and rebuild union (in a new transaction)
        async with self._conn.transaction():
            # Drop any existing temp table from previous operations in this session
            try:
                await self._conn.execute("DROP TABLE IF EXISTS tmp_provsql")
            except Exception:
                pass  # Ignore any errors

            await self._conn.execute(
                "SELECT create_provenance_mapping(%s, %s, %s)",
                (prov_table, table_name,
                 self._semiring.mappingStrategy.encode(table_name))
            )
            await self._rebuild_union_mapping(schema_name)

    async def remove_provenance_for(self, schema_name: str, table_name: str) -> bool:
        """
        Remove the provenance mapping table for a given base table in the specified schema.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.

        Returns:
            True if the table existed and was dropped, False otherwise.
        """
        prov_table = self._semiring.get_provenance_table_name_for(table_name)
        table_existed = True

        await self._set_search_path(schema_name)

        # Remove the provenance mapping table (separate transaction)
        try:
            async with self._conn.transaction():
                drop_query = (
                    SQL("DROP TABLE IF EXISTS {} CASCADE")
                    .format(Identifier(prov_table))
                )
                await self._conn.execute(drop_query)
        except errors.UndefinedTable:
            table_existed = False

        # Remove the provenance column from the base table (separate transaction)
        try:
            async with self._conn.transaction():
                await self._conn.execute("SELECT remove_provenance(%s)", (table_name,))
        except errors.UndefinedColumn:
            logger.info(
                f"Table '{table_name}' has no provenance column, ignoring")

        # Rebuild union mapping (separate transaction)
        async with self._conn.transaction():
            await self._rebuild_union_mapping(schema_name)

        return table_existed

    async def _rebuild_union_mapping(self, schema_name: str) -> bool:
        """
        Build or rebuild a union table containing all records of all provenance mapping tables for the semiring in the schema.
        """
        await self._set_search_path(schema_name)

        cursor = await self._conn.cursor(row_factory=dict_row).execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s AND tablename LIKE %s",
            (schema_name, f"%{self._semiring.table_suffix}")
        )
        provwhy_tables = await cursor.fetchall()

        if not provwhy_tables:
            logger.warning(
                f"No tables ending with {self._semiring.table_suffix} found")
            return False

        # Create the union mapping table
        name = self._semiring.union_table_name

        await self._conn.execute(SQL("DROP TABLE IF EXISTS {} CASCADE").format(Identifier(name)))

        union_query = " UNION ".join([
            f"SELECT * FROM {row['tablename']}" for row in provwhy_tables
        ])
        composed_rq = SQL("CREATE TABLE {} AS {}").format(
            Identifier(name),
            SQL(cast(LiteralString, union_query))
        )
        await self._conn.execute(composed_rq)

        # Adjust the value column to be an Array and add primary key
        # TODO : This is semiring specific, should be abstracted
        await self._conn.execute(SQL("ALTER TABLE {} ALTER COLUMN value TYPE varchar").format(Identifier(name)))
        await self._conn.execute(SQL("UPDATE {} SET value = '{{\"{{' || value || '}}\"}}'").format(Identifier(name)))
        await self._conn.execute(SQL("ALTER TABLE {} ADD PRIMARY KEY (provenance)").format(Identifier(name)))

        logger.info(
            f"Created {name} table from {len(provwhy_tables)} {self._semiring.table_suffix} tables")
        return True

    async def _set_search_path(self, schema_name: str) -> None:
        """Set the PostgreSQL search path for the current connection."""
        query = SQL("SET search_path TO {}, public, provsql;").format(
            Identifier(schema_name)
        )
        await self._conn.execute(query)

    async def _fetch_related_data(self, provenance: str, semiring: DbSemiring) -> list[dict]:
        matches = semiring.mappingStrategy.decode_equation(provenance)

        # Group by table
        table_groups = defaultdict(list)
        for row in matches:
            table_groups[row['table']].append(row)

        results = []

        # Query each table for the relevant rows
        for table, rows in table_groups.items():
            ctids = [f"({r['page']},{r['row']})" for r in rows]

            query = (
                SQL("SELECT *, ctid FROM {} WHERE ctid = ANY(%s)")
                .format(Identifier(table))
            )
            cursor = await self._conn.cursor(row_factory=dict_row).execute(query, (ctids,))
            data_by_ctid = {
                str(r['ctid']): r for r in await cursor.fetchall()
            }

            for r in rows:
                ctid = f"({r['page']},{r['row']})"
                if row := data_by_ctid.get(ctid):
                    row = dict(row)
                    row.pop('ctid', None)

                    results.append({
                        "reference": f"{table}@p{r['page']}r{r['row']}",
                        "data": row,
                    })
                else:
                    logger.warning(
                        "No data found for %s with ctid %s", table, ctid)

        return results
