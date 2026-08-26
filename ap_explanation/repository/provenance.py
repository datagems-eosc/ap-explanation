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
from ap_explanation.repository.mapping.key_mapping import REFERENCE_COLUMN
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

    def __init__(self, conn: AsyncConnection, sql_rewriter: SqlRewriter):
        self._conn = conn
        self._sql_rewriter = sql_rewriter

    @classmethod
    async def create(
        cls, conn: AsyncConnection, sql_rewriter: SqlRewriter
    ) -> "ProvenanceRepository":
        repo = cls(conn, sql_rewriter)
        # Enabled per connection, never server-wide: the server default stays
        # provsql.active = 0 (set in the image's postgresql.conf) so that other
        # services querying the same database are never subject to ProvSQL's
        # rewriter, which does not support every SQL construct.
        await conn.execute("SET provsql.active = 1")
        return repo

    async def query(
        self, schema_name: str, query: str, semiring: DbSemiring
    ) -> list[ProvSQLRow]:
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
                cursor = await self._conn.cursor(row_factory=dict_row).execute(
                    SQL(cast(LiteralString, edited_query))
                )
                rows = await cursor.fetchall()

                # Pause ProvSQL before resolving references. Those lookups run
                # against tracked tables, and leaving it on rewrites them,
                # appends a provsql column to the payload and warns about
                # constructs it cannot track. SET LOCAL reverts at commit, so
                # the connection goes back to active for the next query.
                await self._conn.execute("SET LOCAL provsql.active = 0")

                results: list[ProvSQLRow] = []
                for row in rows:
                    expression = row.get(semiring.retrieval_function, "")
                    data = await self._fetch_related_data(expression, semiring)

                    # Build the answer dict: everything except provenance-internal columns
                    provenance_keys = {
                        "provsql",
                        REFERENCE_COLUMN,
                        semiring.retrieval_function,
                        semiring.name,
                    }

                    answer = {k: v for k, v in row.items() if k not in provenance_keys}

                    results.append(
                        ProvSQLRow(
                            answer=answer,
                            provsql=str(row.get("provsql", "")),
                            provenance=SemiringProvenance(
                                expression=expression,
                                data=data,
                            ),
                        )
                    )

            return results
        except errors.UndefinedTable as e:
            # The mapping table doesn't exist, meaning the table hasn't been annotated
            from ap_explanation.errors import TableNotAnnotatedError

            logger.warning(f"Table not annotated with semiring '{semiring.name}': {e}")
            raise TableNotAnnotatedError(
                schema_name=schema_name, semiring_name=semiring.name
            ) from e
        except errors.InternalError_ as e:
            # ProvSQL internal error, typically when provenance functions are called on non-annotated data
            logger.error(
                f"ProvSQL internal error while querying with semiring '{semiring.name}': {e}"
            )
            raise ProvSqlInternalError(
                f"ProvSQL internal error occurred. The table may have lost its provenance annotations. "
                f"Please re-annotate the table with the '{semiring.name}' semiring and try again. "
                f"Error details: {str(e)}"
            ) from e

    async def enable_provenance(self, schema_name: str, table_name: str) -> bool:
        """
        Create the provenance annotations for a given base table in the specified schema.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.
        Returns:
            True if the table was newly annotated, False if it was already annotated.
        """
        # ProvSQL's add_provenance is idempotent (1.9.0+), but its
        # NOTICE-and-no-op path is indistinguishable from a real annotation, so
        # the existence check is what computes this method's return value.
        async with self._conn.transaction():
            cursor = await self._conn.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = %s AND table_name = %s AND column_name = 'provsql')",
                (schema_name, table_name),
            )
            result = await cursor.fetchone()
            already_annotated = result[0] if result else False

        if already_annotated:
            logger.info(
                f"Provenance column for table '{table_name}' already exists, skipping"
            )
            return False

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

                    await self._conn.execute(
                        "CREATE EXTENSION IF NOT EXISTS provsql CASCADE"
                    )
                await self._set_search_path(schema_name)
                await self._conn.execute("SELECT add_provenance(%s)", (table_name,))
        except (errors.UndefinedFile, errors.FeatureNotSupported) as e:
            logger.error(
                f"ProvSQL extension is not installed on the postgres server: {e}"
            )
            raise ProvSqlMissingError(
                f"ProvSQL extension is not installed or not available: {str(e)}"
            ) from e
        except errors.UndefinedTable as e:
            logger.warning(
                f"Table '{table_name}' does not exist in schema '{schema_name}': {e}"
            )
            from ap_explanation.errors import TableOrSchemaNotFoundError

            raise TableOrSchemaNotFoundError(
                table_name=table_name, schema_name=schema_name
            ) from e

        return True

    async def add_semiring(
        self, schema_name: str, table_name: str, semiring: DbSemiring
    ) -> bool:
        """
        Add a semiring's provenance annotations to an existing table that have provenance enabled.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.
        Returns:
            True if the semiring's mapping was newly created (or rebuilt from an
            outdated format), False if it was already in place.
        """
        prov_table = semiring.get_provenance_table_name_for(table_name)
        strategy = semiring.mappingStrategy

        await self._set_search_path(schema_name)

        # ProvSQL's create_provenance_mapping is idempotent (1.9.0+), but its
        # NOTICE-and-no-op path is indistinguishable from a real creation, so
        # the existence check is what computes this method's return value.
        async with self._conn.transaction():
            cursor = await self._conn.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s)",
                (schema_name, prov_table),
            )
            result = await cursor.fetchone()
            table_already_exists = result[0] if result else False

        if table_already_exists and await self._mapping_is_current(
            schema_name, table_name, prov_table, strategy
        ):
            logger.info(
                f"Provenance table '{prov_table}' already exists, skipping creation"
            )
            async with self._conn.transaction():
                await self._rebuild_union_mapping(schema_name, semiring)
            return False

        if table_already_exists:
            # Built by a different mapping strategy (a pre-KeyMapping ctid
            # mapping, or one predating maintained registration): its stored
            # values cannot be converted, so rebuild from the tokens the source
            # table currently holds.
            logger.info(
                f"Provenance table '{prov_table}' is not a current maintained mapping, rebuilding"
            )
            async with self._conn.transaction():
                await self._conn.execute(
                    SQL("DROP TABLE {} CASCADE").format(Identifier(prov_table))
                )

        await self._ensure_reference_column(schema_name, table_name, strategy)

        try:
            async with self._conn.transaction():
                # Needs ProvSQL active (which the connection is): it copies the
                # source table with `TABLE oldtbl`, and ProvSQL hides the
                # provsql column from star expansion unless active, so with it
                # paused the copy has no provenance column and this fails.
                # maintained => true registers the mapping in
                # provenance_mapping_registry, so provenance_guard appends a row
                # for every subsequent INSERT into the source table. Requires
                # ProvSQL >= 1.11.0, and requires the attribute to be a column
                # name: the guard interpolates it as ($1).%I.
                await self._conn.execute(
                    "SELECT create_provenance_mapping(%s, %s, %s, false, true)",
                    (prov_table, table_name, strategy.reference_column),
                )
        except Exception as e:
            logger.error(f"Unexpected error in create_provenance_mapping: {e}")
            raise

        # Rebuild the union mapping view for this semiring
        async with self._conn.transaction():
            await self._rebuild_union_mapping(schema_name, semiring)

        return True

    async def _mapping_is_current(
        self, schema_name: str, table_name: str, prov_table: str, strategy
    ) -> bool:
        """
        Check whether an existing mapping table was built by the current
        strategy, and so can be left alone.

        This is a structural check rather than a check of the stored values:
        the values a mapping built by an older version holds are CTIDs of a
        temporary copy of the table, which identify nothing and cannot be
        converted — such a mapping can only be rebuilt. Sampling a value would
        also mis-classify an empty mapping table (annotating an empty source
        table) as current, leaving it unmaintained forever.
        """
        async with self._conn.transaction():
            cursor = await self._conn.execute(
                "SELECT"
                " EXISTS (SELECT 1 FROM information_schema.columns"
                "         WHERE table_schema = %s AND table_name = %s AND column_name = %s),"
                " EXISTS (SELECT 1 FROM provsql.provenance_mapping_registry"
                "         WHERE mapping = to_regclass(%s)::oid)",
                (
                    schema_name,
                    table_name,
                    strategy.reference_column,
                    f"{schema_name}.{prov_table}",
                ),
            )
            has_reference_column, is_registered = await cursor.fetchone()

        return bool(has_reference_column and is_registered)

    async def _ensure_reference_column(
        self, schema_name: str, table_name: str, strategy
    ) -> None:
        """
        Add the strategy's reference column to a source table.

        A plain column with a DEFAULT rather than a GENERATED column: ProvSQL's
        provenance_guard reads it as ``NEW.<column>`` from a BEFORE INSERT
        trigger, where a generated column is not yet computed.
        """
        column = strategy.reference_column

        await self._set_search_path(schema_name)

        async with self._conn.transaction():
            await self._conn.execute(
                SQL(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} text NOT NULL DEFAULT ({})"
                ).format(
                    Identifier(table_name),
                    Identifier(column),
                    SQL(cast(LiteralString, strategy.encode(table_name))),
                )
            )
            # Deliberately not UNIQUE: ProvSQL's data-modification machinery
            # re-inserts rows carrying their existing column values, which a
            # unique index could reject.
            await self._conn.execute(
                SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                    Identifier(f"{table_name}_{column}_idx"),
                    Identifier(table_name),
                    Identifier(column),
                )
            )

    async def remove_semiring(
        self, schema_name: str, table_name: str, semiring: DbSemiring
    ) -> bool:
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
                (schema_name, prov_table),
            )
            result = await cursor.fetchone()
            table_existed = result[0] if result else False

        # Remove the semiring's provenance mapping table if it exists
        if table_existed:
            async with self._conn.transaction():
                drop_query = SQL("DROP TABLE {} CASCADE").format(Identifier(prov_table))
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

        # ProvSQL >= 1.12.0 removes provenance idempotently and drops the
        # provenance_guard / insert_statement / update_statement /
        # delete_statement triggers itself (the issue-68 workaround this method
        # used to carry).
        async with self._conn.transaction():
            await self._conn.execute("SELECT remove_provenance(%s)", (table_name,))

        # The reference column is ours, not ProvSQL's, so it needs dropping here.
        async with self._conn.transaction():
            await self._conn.execute(
                SQL("ALTER TABLE {} DROP COLUMN IF EXISTS {}").format(
                    Identifier(table_name), Identifier(REFERENCE_COLUMN)
                )
            )

    async def _rebuild_union_mapping(
        self, schema_name: str, semiring: DbSemiring
    ) -> bool:
        """
        Build or rebuild the union relation covering every provenance mapping
        table for this semiring in the schema.

        The semiring retrieval functions (``sr_why(provenance(), 'why_mapping')``)
        take a single regclass, so a query spanning several tables needs one
        relation holding all of their tokens. It cannot be a single shared
        mapping table instead: ``provenance_mapping_registry`` keys on the
        mapping's oid, so one mapping table can only ever serve one source
        table.

        It is a view rather than the materialised snapshot this used to build,
        so that the rows ``provenance_guard`` appends to a maintained mapping
        are visible to queries without re-annotating.
        """
        await self._set_search_path(schema_name)

        cursor = await self._conn.cursor(row_factory=dict_row).execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s AND tablename LIKE %s",
            (schema_name, f"%{semiring.table_suffix}"),
        )
        provwhy_tables = await cursor.fetchall()

        if not provwhy_tables:
            logger.warning(
                f"No tables ending with {semiring.table_suffix} found in schema {schema_name}"
            )
            return False

        # Create the union mapping view with schema-qualified name
        name = semiring.union_table_name
        qualified_name = SQL("{}.{}").format(Identifier(schema_name), Identifier(name))

        # A database annotated by an earlier version has a materialised
        # snapshot under this name rather than a view. DROP VIEW IF EXISTS
        # would raise WrongObjectType on it — IF EXISTS only guards against
        # the name being absent, not against it being another kind — so pick
        # the statement from what is actually there.
        cursor = await self._conn.execute(
            "SELECT relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = %s AND c.relname = %s",
            (schema_name, name),
        )
        existing = await cursor.fetchone()
        if existing is not None:
            statement = (
                "DROP VIEW {} CASCADE"
                if existing[0] == "v"
                else "DROP TABLE {} CASCADE"
            )
            await self._conn.execute(
                SQL(cast(LiteralString, statement)).format(qualified_name)
            )

        # UNION ALL, not UNION: provenance tokens are unique per input row, so
        # deduplicating is pure cost and it stops the planner pushing the
        # evaluator's token join down into each mapping table's index.
        union_query = " UNION ALL ".join(
            [
                f"SELECT value, provenance FROM {schema_name}.{row['tablename']}"
                for row in provwhy_tables
            ]
        )
        composed_rq = SQL("CREATE VIEW {} AS {}").format(
            qualified_name, SQL(cast(LiteralString, union_query))
        )
        await self._conn.execute(composed_rq)

        logger.info(
            f"Created {schema_name}.{name} view from {len(provwhy_tables)} {semiring.table_suffix} tables"
        )
        return True

    async def _set_search_path(self, schema_name: str) -> None:
        """Set the PostgreSQL search path for the current connection."""
        query = SQL("SET search_path TO {}, public, provsql;").format(
            Identifier(schema_name)
        )
        await self._conn.execute(query)

    async def _fetch_related_data(
        self, provenance: str, semiring: DbSemiring
    ) -> list[dict]:
        strategy = semiring.mappingStrategy
        matches = strategy.decode_equation(provenance)

        # Group by table
        table_groups = defaultdict(list)
        for row in matches:
            table_groups[row["table"]].append(row)

        results = []

        # Query each table for the relevant rows
        for table, rows in table_groups.items():
            lookups = [strategy.lookup_value(r) for r in rows]

            # Alias the lookup column so the results can be keyed by it
            # regardless of whether it is a real column or a system one.
            query = SQL(
                "SELECT *, {lookup}::text AS __lookup FROM {table} WHERE {lookup}::text = ANY(%s)"
            ).format(lookup=Identifier(strategy.lookup_column), table=Identifier(table))
            cursor = await self._conn.cursor(row_factory=dict_row).execute(
                query, (lookups,)
            )
            data_by_lookup = {r["__lookup"]: r for r in await cursor.fetchall()}

            for r in rows:
                lookup = strategy.lookup_value(r)
                if row := data_by_lookup.get(lookup):
                    row = dict(row)
                    row.pop("__lookup", None)
                    row.pop(REFERENCE_COLUMN, None)
                    # Belt and braces: this query runs with ProvSQL paused, so
                    # there should be no provenance column to strip.
                    row.pop("provsql", None)

                    results.append(
                        {
                            "reference": strategy.reference(r),
                            "data": row,
                        }
                    )
                else:
                    logger.warning(
                        "No data found for %s with reference %s",
                        table,
                        strategy.reference(r),
                    )

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
                await cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = '{schema_name}'
                    AND table_name = %s
                    ORDER BY ordinal_position
                """,
                    (table_name,),
                )
                columns = await cur.fetchall()

                # 3. Collect column definitions
                col_defs = []
                for col_name, data_type, is_nullable, col_default in columns:
                    nullable = "" if is_nullable == "NO" else " NULL"
                    default = f" DEFAULT {col_default}" if col_default else ""
                    col_defs.append(f"    {col_name} {data_type}{nullable}{default}")

                # 4. Print CREATE TABLE once per table
                sb.append(f"--\n-- Table: {schema_name}.{table_name}\n--")
                sb.append(
                    f"CREATE TABLE {schema_name}.{table_name} (\n"
                    + ",\n".join(col_defs)
                    + "\n);\n"
                )

                # 5. Add primary key once per table
                await cur.execute(
                    """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = '{schema_name}'
                    AND tc.table_name = %s
                    AND tc.constraint_type = 'PRIMARY KEY'
                    ORDER BY kcu.ordinal_position
                """,
                    (table_name,),
                )
                pk_columns = [row[0] for row in await cur.fetchall()]
                if pk_columns:
                    pk_cols = ", ".join(pk_columns)
                    sb.append(
                        f"ALTER TABLE {schema_name}.{table_name} ADD PRIMARY KEY ({pk_cols});\n"
                    )

        return "\n".join(sb)
