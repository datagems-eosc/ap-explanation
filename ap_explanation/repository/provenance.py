from collections import defaultdict
from logging import getLogger
from typing import LiteralString, cast

from orjson import dumps
from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg.types.json import set_json_dumps

from ap_explanation.errors import ProvSqlInternalError, ProvSqlMissingError
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.types.provenance import ProvSQLRow, SemiringProvenance
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

    @classmethod
    async def create(cls, conn: AsyncConnection, sql_rewriter: SqlRewriter) -> "ProvenanceRepository":
        repo = cls(conn, sql_rewriter)
        await conn.execute("SET provsql.active = 1")
        return repo

    async def query(self, schema_name: str, query: str, semiring: DbSemiring) -> list[ProvSQLRow]:
        """
        Execute a SQL query with provenance tracking and return structured results.

        Each returned row separates the original query output (``answer``) from
        provenance metadata so that results from different semirings can be
        merged cleanly by the service layer.

        Returns:
            A list of dicts, each with keys:
            - ``answer``     – dict of the original query columns
            - ``provsql``    – the provenance UUID for this row
            - ``expression`` – the raw semiring expression string
            - ``data``       – resolved provenance references (list of dicts)
        """
        edited_query = self._sql_rewriter.rewrite(query, semiring)

        try:
            async with self._conn.transaction():
                await self._set_search_path(schema_name)

                # Fetch the provenance-annotated results
                cursor = await self._conn.cursor(row_factory=dict_row).execute(SQL(cast(LiteralString, edited_query)))
                rows = await cursor.fetchall()

                results: list[ProvSQLRow] = []
                for row in rows:
                    expression = row.get(semiring.retrieval_function, "")
                    data = await self._fetch_related_data(expression, semiring)

                    # Build the answer dict: everything except provenance-internal columns
                    provenance_keys = {
                        "provsql",
                        semiring.retrieval_function,
                        semiring.name,
                    }

                    answer = {k: v for k, v in row.items(
                    ) if k not in provenance_keys}

                    results.append(ProvSQLRow(
                        answer=answer,
                        provsql=str(row.get("provsql", "")),
                        provenance=SemiringProvenance(
                            expression=expression,
                            data=data,
                        ),
                    ))

            return results
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
        newly_annotated = True
        try:
            async with self._conn.transaction():
                # Check if provsql is already installed
                cursor = await self._conn.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'provsql')"
                )
                provsql_installed = (await cursor.fetchone())[0]

                if not provsql_installed:
                    # provsql is not installed — verify the current user is a superuser before attempting
                    cursor = await self._conn.execute(
                        "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
                    )
                    row = await cursor.fetchone()
                    is_superuser = row[0] if row else False

                    if not is_superuser:
                        raise ProvSqlMissingError(
                            "ProvSQL extension is not installed and the current database user is not a superuser. "
                            "Please ask a superuser to install the extension with: CREATE EXTENSION provsql CASCADE"
                        )

                    await self._conn.execute("CREATE EXTENSION IF NOT EXISTS provsql CASCADE")
                await self._set_search_path(schema_name)
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

        # Workaround for https://github.com/PierreSenellart/provsql/issues/68
        async with self._conn.transaction():
            drop_insert_trigger = SQL("DROP TRIGGER IF EXISTS insert_statement ON {} CASCADE").format(
                Identifier(table_name))
            drop_delete_trigger = SQL("DROP TRIGGER IF EXISTS delete_statement ON {} CASCADE").format(
                Identifier(table_name))
            drop_update_trigger = SQL("DROP TRIGGER IF EXISTS update_statement ON {} CASCADE").format(
                Identifier(table_name))

            await self._conn.execute(drop_insert_trigger)
            await self._conn.execute(drop_delete_trigger)
            await self._conn.execute(drop_update_trigger)

        # Then try to remove the provenance column
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
        qualified_name = SQL("{}.{}").format(
            Identifier(schema_name), Identifier(name))

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

    async def get_schema_definition(self, schema_name: str) -> str:
        """
        Retrieve schema information such as table and column names for the specified schema.

        Args:
            schema_name: The name of the database schema to retrieve information for.
        Returns:
            A string containing schema information, including tables, columns, and relationships.
        """
        # NOTE: The simplest way to do this would be to use pg_dump with postgres.
        # However, we use this kind of convoluted method to be able to support a large panel of databases.
        # This may not be sufficient to support all db, in which case we can consider implementing multiple strategies for schema retrieval

        # Pseudo string builder for schema info, can be replaced with structured dict if needed
        sb = []
        async with self._conn.cursor() as cur:
            # 1. Get all tables in the schema
            await cur.execute(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema_name}'
                AND table_type='BASE TABLE'
                ORDER BY table_name
            """)
            tables = [row[0] for row in await cur.fetchall()]

            for table_name in tables:
                # 2. Get columns for this table
                await cur.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = '{schema_name}'
                    AND table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                columns = await cur.fetchall()

                # 3. Collect column definitions
                col_defs = []
                for col_name, data_type, is_nullable, col_default in columns:
                    nullable = "" if is_nullable == "NO" else " NULL"
                    default = f" DEFAULT {col_default}" if col_default else ""
                    col_defs.append(
                        f"    {col_name} {data_type}{nullable}{default}")

                # 4. Print CREATE TABLE once per table
                sb.append(f"--\n-- Table: {schema_name}.{table_name}\n--")
                sb.append(
                    f"CREATE TABLE {schema_name}.{table_name} (\n" + ",\n".join(col_defs) + "\n);\n")

                # 5. Add primary key once per table
                await cur.execute("""
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = '{schema_name}'
                    AND tc.table_name = %s
                    AND tc.constraint_type = 'PRIMARY KEY'
                    ORDER BY kcu.ordinal_position
                """, (table_name,))
                pk_columns = [row[0] for row in await cur.fetchall()]
                if pk_columns:
                    pk_cols = ", ".join(pk_columns)
                    sb.append(
                        f"ALTER TABLE {schema_name}.{table_name} ADD PRIMARY KEY ({pk_cols});\n")

        return "\n".join(sb)
