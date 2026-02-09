from collections import defaultdict
from logging import getLogger
from typing import Any, LiteralString, cast

from orjson import dumps
from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg.types.json import set_json_dumps

from ap_explanation.errors import ProvSqlInternalError, ProvSqlMissingError
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.types.semiring import DbSemiring

logger = getLogger(__name__)
set_json_dumps(dumps)


class ProvenanceRepository:
    """
    Repository for all provenance-related operations.

    Handles both provenance setup (annotations) and querying with provenance tracking.
    """
    _conn: AsyncConnection
    _sql_rewriter: SqlRewriter

    def __init__(
        self,
        conn: AsyncConnection,
        sql_rewriter: SqlRewriter
    ):
        self._conn = conn
        self._sql_rewriter = sql_rewriter

    async def query(self, schema_name: str, query: str, semiring: DbSemiring) -> list[dict[str, Any]]:
        """
        Execute a SQL query and return the results as a list of dictionaries.
        :param query: The SQL query to execute.
        :return: List of result rows as dictionaries.
        """
        edited_query = self._sql_rewriter.rewrite(query, semiring)

        try:
            async with self._conn.transaction():
                await self._set_search_path(schema_name)

                # Fetch the provenance-annotated results
                cursor = await self._conn.cursor(row_factory=dict_row).execute(SQL(cast(LiteralString, edited_query)))
                rows = await cursor.fetchall()

                # From each row, retrieve the provenance data
                for row in rows:
                    retrieval_name = semiring.retrieval_function
                    if semiring.aggregate_function is not None and semiring.aggregate_function in row:
                        retrieval_name = semiring.aggregate_function
                    row[semiring.name] = await self._fetch_related_data(row[retrieval_name], semiring)

            return rows
        except errors.UndefinedTable as e:
            # The mapping table doesn't exist, meaning the table hasn't been annotated
            from ap_explanation.errors import TableNotAnnotatedError
            logger.warning(
                f"Table not annotated with semiring '{semiring.name}': {e}")
            raise TableNotAnnotatedError(
                schema_name=schema_name, semiring_name=semiring.name) from e
        except errors.InternalError_ as e:
            # ProvSQL internal error, typically when provenance functions are called on non-annotated data
            logger.error(
                f"ProvSQL internal error while querying with semiring '{semiring.name}': {e}")
            raise ProvSqlInternalError(
                f"ProvSQL internal error occurred. The table may have lost its provenance annotations. "
                f"Please re-annotate the table with the '{semiring.name}' semiring and try again. "
                f"Error details: {str(e)}") from e

    async def enable_provenance(self, schema_name: str, table_name: str) -> bool:
        """
        Create the provenance annotations for a given base table in the specified schema.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.
        Returns:
            True if the table was newly annotated, False if it was already annotated.
        """
        await self._set_search_path(schema_name)
        newly_annotated = True
        try:
            async with self._conn.transaction():
                await self._conn.execute("CREATE EXTENSION IF NOT EXISTS provsql CASCADE")
                await self._conn.execute("SELECT add_provenance(%s)", (table_name,))
        except (errors.UndefinedFile, errors.FeatureNotSupported) as e:
            logger.error(
                f"ProvSQL extension is not installed on the postgres server: {e}")
            raise ProvSqlMissingError(
                f"ProvSQL extension is not installed or not available: {str(e)}"
            ) from e
        except errors.UndefinedTable as e:
            logger.warning(
                f"Table '{table_name}' does not exist in schema '{schema_name}': {e}")
            from ap_explanation.errors import TableOrSchemaNotFoundError
            raise TableOrSchemaNotFoundError(
                table_name=table_name, schema_name=schema_name) from e
        except errors.DuplicateColumn:
            logger.info(
                f"Provenance column for table '{table_name}' already exists, ignoring")
            newly_annotated = False

        return newly_annotated

    async def ensure_semiring_setup(self, required_version: str = "1.0.0") -> None:
        """
        Check if the semiring setup script has been executed and run it if needed.
        Uses a canary table to track execution status.

        Args:
            required_version: The version of the script that should be present.
        """
        script_name = "03_setup_semiring_parallel.sql"

        # Check if canary table exists and has the correct version
        needs_execution = False

        try:
            async with self._conn.transaction():
                cursor = await self._conn.execute(
                    """
                    SELECT version FROM public.provsql_canary 
                    WHERE script_name = %s
                    """,
                    (script_name,),
                )
                result = await cursor.fetchone()

                if result is None:
                    logger.info(
                        f"Canary not found for {script_name}, will execute script"
                    )
                    needs_execution = True
                elif result[0] != required_version:
                    logger.info(
                        f"Version mismatch for {script_name}: found {result[0]}, expected {required_version}, will re-execute"
                    )
                    needs_execution = True
                else:
                    logger.debug(
                        f"Semiring setup already executed (version {result[0]})"
                    )
        except errors.UndefinedTable:
            logger.info(
                f"Canary table does not exist, will execute {script_name}")
            needs_execution = True

        if needs_execution:
            await self._execute_semiring_setup_script()

    async def _execute_semiring_setup_script(self) -> None:
        """
        Execute the semiring setup script from the repository resources directory.
        """
        from pathlib import Path

        # Find the script file relative to this module
        script_path = Path(__file__).parent / "resources" / \
            "03_setup_semiring_parallel.sql"

        if not script_path.exists():
            logger.error(f"Semiring setup script not found at {script_path}")
            raise FileNotFoundError(
                f"Required script not found: {script_path}")

        logger.info(f"Executing semiring setup script: {script_path}")

        # Read and execute the script
        script_content = script_path.read_text(encoding='utf-8')

        try:
            async with self._conn.transaction():
                await self._conn.execute(SQL(cast(LiteralString, script_content)))
            logger.info("Semiring setup script executed successfully")
        except Exception as e:
            logger.error(f"Failed to execute semiring setup script: {e}")
            raise

    async def add_semiring(self, schema_name: str, table_name: str, semiring: DbSemiring) -> bool:
        """
        Add a semiring's provenance annotations to an existing table that have provenance enabled.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.
        Returns:
            True if the semiring's was already active for the table, False if it was newly created.
        """
        prov_table = semiring.get_provenance_table_name_for(table_name)

        await self._set_search_path(schema_name)

        # Drop any existing temp table from previous operations
        # ProvSQl can leave temp tables behind if an error occurs
        async with self._conn.transaction():
            try:
                await self._conn.execute("DROP TABLE IF EXISTS tmp_provsql")
            except Exception:
                pass

        # Attempt to create the semiring's provenance mapping table.
        # If it already exists, the semiring is already active.
        # We need to handle DuplicateTable carefully because it leaves the transaction in a failed state.
        semiring_created = True
        try:
            async with self._conn.transaction():
                await self._conn.execute(
                    "SELECT create_provenance_mapping(%s, %s, %s)",
                    (prov_table, table_name,
                     semiring.mappingStrategy.encode(table_name))
                )
        except errors.DuplicateTable:
            logger.info(
                f"Provenance table '{prov_table}' already exists, ignoring")
            semiring_created = False
        except Exception as e:
            logger.error(f"Unexpected error in create_provenance_mapping: {e}")
            raise

        # Rebuild the union mapping table for this semiring
        async with self._conn.transaction():
            await self._rebuild_union_mapping(schema_name, semiring)

        return semiring_created

    async def remove_semiring(self, schema_name: str, table_name: str, semiring: DbSemiring) -> bool:
        """
        Remove a semiring's provenance annotations from an existing table that have provenance enabled.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.
        Returns:
            True if the semiring's provenance table existed and was dropped, False otherwise.

        """
        prov_table = semiring.get_provenance_table_name_for(table_name)

        await self._set_search_path(schema_name)

        # Check if the provenance table exists before attempting to drop it
        async with self._conn.transaction():
            cursor = await self._conn.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s)",
                (schema_name, prov_table)
            )
            result = await cursor.fetchone()
            table_existed = result[0] if result else False

        # Remove the semiring's provenance mapping table if it exists
        if table_existed:
            async with self._conn.transaction():
                drop_query = (
                    SQL("DROP TABLE {} CASCADE")
                    .format(Identifier(prov_table))
                )
                await self._conn.execute(drop_query)

            async with self._conn.transaction():
                await self._rebuild_union_mapping(schema_name, semiring)

        return table_existed

    async def remove_provenance(self, schema_name: str, table_name: str) -> None:
        """
        Remove the provenance mapping table for a given base table in the specified schema.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.
        """

        await self._set_search_path(schema_name)
        try:
            async with self._conn.transaction():
                await self._conn.execute("SELECT remove_provenance(%s)", (table_name,))
        except errors.UndefinedColumn:
            logger.info(
                f"Table '{table_name}' has no provenance column, ignoring")

    async def _rebuild_union_mapping(self, schema_name: str, semiring: DbSemiring) -> bool:
        """
        Build or rebuild a union table containing all records of all provenance mapping tables for the semiring in the schema.
        """
        await self._set_search_path(schema_name)

        cursor = await self._conn.cursor(row_factory=dict_row).execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s AND tablename LIKE %s",
            (schema_name, f"%{semiring.table_suffix}")
        )
        provwhy_tables = await cursor.fetchall()

        if not provwhy_tables:
            logger.warning(
                f"No tables ending with {semiring.table_suffix} found in schema {schema_name}")
            return False

        # Create the union mapping table with schema-qualified name
        name = semiring.union_table_name
        qualified_name = SQL("{}.{}").format(Identifier(schema_name), Identifier(name))

        await self._conn.execute(SQL("DROP TABLE IF EXISTS {} CASCADE").format(qualified_name))

        # Build union query with schema-qualified table names
        union_query = " UNION ".join([
            f"SELECT * FROM {schema_name}.{row['tablename']}" for row in provwhy_tables
        ])
        composed_rq = SQL("CREATE TABLE {} AS {}").format(
            qualified_name,
            SQL(cast(LiteralString, union_query))
        )
        await self._conn.execute(composed_rq)

        # Adjust the value column to be an Array and add primary key
        # NOTE : This may be semiring specific, should be abstracted
        await self._conn.execute(SQL("ALTER TABLE {} ALTER COLUMN value TYPE varchar").format(qualified_name))
        await self._conn.execute(SQL("UPDATE {} SET value = '{{\"{{' || value || '}}\"}}'").format(qualified_name))
        await self._conn.execute(SQL("ALTER TABLE {} ADD PRIMARY KEY (provenance)").format(qualified_name))

        logger.info(
            f"Created {schema_name}.{name} table from {len(provwhy_tables)} {semiring.table_suffix} tables")
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
