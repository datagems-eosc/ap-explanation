from collections import defaultdict
from contextlib import AbstractAsyncContextManager, nullcontext
from logging import getLogger
from typing import LiteralString, cast

from orjson import dumps
from psycopg import AsyncConnection, Rollback, errors
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg.types.json import set_json_dumps

from ap_explanation.errors import (
    ProvSqlInternalError,
    QueryLimitExceededError,
    TableNotAnnotatedError,
)
from ap_explanation.internal.distributed_lock import RedisRowBudget
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repository.provsql import (
    REFERENCE_COLUMN,
    references_in,
    set_search_path,
)
from ap_explanation.settings import QueryLimits
from ap_explanation.types.provenance import Derivation, SemiringProvenance
from ap_explanation.types.semiring import DbSemiring

logger = getLogger(__name__)
set_json_dumps(dumps)


class QueryRepository:
    """
    Runs SQL queries on annotated tables, with provenance tracking.
    """

    _conn: AsyncConnection
    _sql_rewriter: SqlRewriter
    _limits: QueryLimits
    #: Whether ProvSQL writes formulas as rows (sr_formula_rows, ProvSQL 1.14)
    _formula_rows: bool = False

    #: Where an aggregate query's result is kept, for the rows of its formulas
    #: to be read from its tokens, valid in this transaction only
    RESULT_TABLE = "apx_result"

    def __init__(
        self,
        conn: AsyncConnection,
        sql_rewriter: SqlRewriter,
        limits: QueryLimits | None = None,
        budget: RedisRowBudget | None = None,
    ):
        self._conn = conn
        self._sql_rewriter = sql_rewriter
        self._limits = limits or QueryLimits()
        self._budget = budget

    @classmethod
    async def create(
        cls,
        conn: AsyncConnection,
        sql_rewriter: SqlRewriter,
        limits: QueryLimits | None = None,
        budget: RedisRowBudget | None = None,
    ) -> QueryRepository:
        repo = cls(conn, sql_rewriter, limits, budget)
        cursor = await conn.execute("SELECT to_regproc('provsql.sr_formula_rows') IS NOT NULL")
        repo._formula_rows = (await cursor.fetchone())[0]
        return repo

    async def query(
        self,
        schema_name: str,
        query: str,
        semirings: list[DbSemiring],
        compute_probability: bool = False,
    ) -> list[Derivation]:
        """
        Execute a SQL query with provenance tracking and return structured results.

        Every semiring, and the probability, is evaluated in the same query:
        ProvSQL's token for a result row is not stable across runs, so the rows
        of separate runs could not be matched up.

        Each returned row separates the original query output (``answer``) from
        its provenance in each semiring. On an aggregate query, only the
        semirings that support aggregates are evaluated, and with ProvSQL 1.14
        only the first rows of the aggregate's formula are read
        (``max_formula_rows``): the whole formula holds a term per aggregated row.

        Args:
            schema_name: Schema where the query should be executed
            query: The SQL query to execute with provenance
            semirings: Semirings to evaluate each result row in
            compute_probability: Also evaluate each result row's probability.
                Tuple probabilities must have been set with :meth:`set_probabilities`

        Returns:
            One :class:`Derivation` per result row, in the database's order.

        Raises:
            QueryLimitExceededError: If the query ran past one of the
                repository's :class:`QueryLimits`
        """
        # The semirings whose aggregate formula is read as rows
        rows_semirings = (
            [s for s in semirings if s.rows_function and s.supports_aggregates]
            if self._formula_rows and self._sql_rewriter.is_aggregate(query) else []
        )
        edited_query = self._sql_rewriter.rewrite(query, semirings, compute_probability, bool(rows_semirings))
        internal_columns = {
            "provsql",
            REFERENCE_COLUMN,
            SqlRewriter.PROBABILITY_FUNCTION,
            *(s.retrieval_function for s in semirings),
            *(c for s in rows_semirings for c in (s.rows_function, f"{s.rows_function}_token")),
        }

        try:
            await set_search_path(self._conn, schema_name)
            async with self._conn.transaction():
                await self._apply_limits()
                aggregated_rows = await self._check_aggregated_rows(query)

            # Waiting for the server's budget outside a transaction: an idle one
            # would hold back VACUUM, or be ended by idle_in_transaction_session_timeout
            async with self._reserve(aggregated_rows), self._conn.transaction():
                await self._apply_limits()

                if rows_semirings:
                    # An aggregate's token is only valid in the statement that built it, or
                    # in a table that statement wrote: the rows of its formula are read
                    # from there, one aggregate at a time
                    await self._conn.execute(SQL(cast(LiteralString,
                        f"CREATE TEMP TABLE {self.RESULT_TABLE} ON COMMIT DROP AS {edited_query}")))
                    await self._conn.execute("SET LOCAL provsql.active = 0")
                    tokens = ", ".join(f"{s.rows_function}::uuid AS {s.rows_function}_token" for s in rows_semirings)
                    cursor = await self._conn.cursor(row_factory=dict_row).execute(
                        SQL(cast(LiteralString, f"SELECT *, {tokens} FROM {self.RESULT_TABLE}"))
                    )
                else:
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

                results: list[Derivation] = []
                for row in rows:
                    provenance = {}
                    # A semiring the rewriting left out (aggregate) has no column
                    for semiring in semirings:
                        if semiring in rows_semirings:
                            expression, truncated = await self._formula_rows_of(
                                row[f"{semiring.rows_function}_token"], semiring)
                            provenance[semiring.name] = SemiringProvenance(
                                expression=expression,
                                data=await self._fetch_related_data(expression),
                                truncated=truncated,
                            )
                        elif semiring.retrieval_function in row:
                            # NULL for an aggregate over no row
                            expression = row[semiring.retrieval_function] or ""
                            provenance[semiring.name] = SemiringProvenance(
                                expression=expression,
                                data=await self._fetch_related_data(expression),
                            )

                    results.append(
                        Derivation(
                            answer={k: v for k, v in row.items() if k not in internal_columns},
                            provenance=provenance,
                            probability=row.get(SqlRewriter.PROBABILITY_FUNCTION),
                        )
                    )

            return results
        except errors.UndefinedTable as e:
            # The mapping table doesn't exist, meaning the table hasn't been annotated
            names = ", ".join(s.name for s in semirings)
            logger.warning(f"Table not annotated with semiring '{names}': {e}")
            raise TableNotAnnotatedError(
                schema_name=schema_name, semiring_name=names
            ) from e
        except errors.QueryCanceled as e:
            # 57014: statement_timeout, or a pg_cancel_backend by an operator,
            # which is no limit
            if "statement timeout" not in str(e):
                raise
            raise QueryLimitExceededError(
                "statement_timeout", self._limits.statement_timeout, str(e)
            ) from e
        except errors.ProgramLimitExceeded as e:
            # 54000: provsql.max_memory (DETAIL: provsql-reason: memory-limit),
            # or another of ProvSQL's bounds such as provsql.max_worlds
            if "memory" in f"{e} {e.diag.message_detail}":
                raise QueryLimitExceededError(
                    "provsql.max_memory", self._limits.max_memory, str(e)
                ) from e
            raise QueryLimitExceededError("a ProvSQL limit", None, str(e)) from e
        except errors.ConfigurationLimitExceeded as e:
            # 53400: temp_file_limit
            raise QueryLimitExceededError(
                "temp_file_limit", self._limits.temp_file_limit, str(e)
            ) from e
        except errors.InternalError_ as e:
            # ProvSQL internal error, typically when provenance functions are called on non-annotated data
            logger.error(f"ProvSQL internal error while querying with provenance: {e}")
            raise ProvSqlInternalError(
                f"ProvSQL internal error occurred. The table may have lost its provenance annotations. "
                f"Please re-annotate the table and try again. "
                f"Error details: {e!s}"
            ) from e

    async def _formula_rows_of(self, token: object, semiring: DbSemiring) -> tuple[str, bool]:
        """
        The first ``max_formula_rows`` rows of the formula of aggregate *token*,
        one per line, and whether it has more.

        ``sr_formula_rows`` writes the formula as rows of bounded size, ``#1``
        first, and computes them one at a time: the LIMIT stops it. A formula
        of a single row is returned as it is.
        """
        if token is None:  # an aggregate over no row
            return "", False
        limit = self._limits.max_formula_rows
        cursor = await self._conn.execute(
            SQL("SELECT (f.r).name, (f.r).formula FROM ("
                "SELECT provsql.{}(%s::uuid, %s::regclass) AS r LIMIT %s) AS f").format(
                Identifier(cast(str, semiring.rows_function))),
            (token, semiring.union_table_name, limit + 1),
        )
        rows = await cursor.fetchall()
        if len(rows) == 1:
            return rows[0][1], False
        return "\n".join(f"{name} = {formula}" for name, formula in rows[:limit]), len(rows) > limit

    async def _check_aggregated_rows(self, query: str) -> int:
        """
        The rows the aggregates of *query* receive, refusing it past
        ``max_aggregated_rows``, before its provenance is built.

        PostgreSQL's estimates cannot tell: on joins on expressions
        (``ROUND(CAST(latitude AS DECIMAL), 1)``) it estimates 1 row where an
        aggregate gets 24 million. So the query runs once without provenance,
        under ``EXPLAIN ANALYZE`` and ``precheck_timeout``, and the actual rows
        going into each aggregate are read from the plan. Without provenance
        this costs a fraction of the provenance query.

        Returns:
            The rows of all its aggregates: each builds a gate per row.
            0 without an aggregate, or with ``max_aggregated_rows`` unset

        Raises:
            QueryLimitExceededError: If its aggregates receive too many rows,
                or the plain query runs past ``precheck_timeout``
        """
        limit = self._limits.max_aggregated_rows
        if limit is None:
            return 0
        rows = 0
        try:
            # A savepoint rolled back on the way out, undoing the SET LOCALs
            async with self._conn.transaction():
                await self._conn.execute("SET LOCAL provsql.active = 0")
                await self._conn.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (self._limits.precheck_timeout,),
                )
                cursor = await self._conn.execute(SQL(cast(LiteralString, f"EXPLAIN (FORMAT JSON) {query}")))
                # Without an aggregate in its plan, the query has nothing to count: no need to run it
                if _aggregated_rows((await cursor.fetchone())[0][0]["Plan"]):
                    cursor = await self._conn.execute(
                        SQL(cast(LiteralString, f"EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) {query}"))
                    )
                    rows = sum(_aggregated_rows((await cursor.fetchone())[0][0]["Plan"]))
                raise Rollback()
        except errors.QueryCanceled as e:
            if "statement timeout" not in str(e):
                raise
            raise QueryLimitExceededError(
                "the plain query's statement_timeout", self._limits.precheck_timeout,
                "the query takes too long even without provenance to explain it",
            ) from e

        if rows > limit:
            raise QueryLimitExceededError(
                "max_aggregated_rows", f"{limit:,}",
                f"the aggregates of the query receive {rows:,} rows",
            )
        return rows

    def _reserve(self, aggregated_rows: int) -> AbstractAsyncContextManager[None]:
        """
        Hold the rows of the provenance query out of its server's
        ``max_aggregated_rows``, shared with the queries of all its databases,
        waiting until the others leave enough.
        """
        budget = self._limits.max_aggregated_rows
        if self._budget is None or budget is None:
            return nullcontext()
        least = self._limits.min_reserved_rows
        rows = max(aggregated_rows, budget // 8 if least is None else least)
        info = self._conn.info
        return self._budget.reserve(f"{info.host}:{info.port}", min(rows, budget), budget)

    async def _apply_limits(self) -> None:
        """
        Apply the repository's :class:`QueryLimits` to the current transaction.

        ``set_config(..., true)`` is ``SET LOCAL`` with bound parameters: the
        settings revert at commit, so annotation and the other services on the
        database keep the server's values. Each one is set in its own
        savepoint so that a parameter the user may not set (temp_file_limit
        needs superuser or GRANT SET) is skipped rather than failing the query.
        """
        for name, value in self._limits.settings().items():
            try:
                async with self._conn.transaction():
                    await self._conn.execute(
                        "SELECT set_config(%s, %s, true)", (name, value)
                    )
            except errors.InsufficientPrivilege as e:
                logger.warning(
                    f"Cannot set {name} = {value}, leaving the server's value: {e}"
                )

    async def _fetch_related_data(self, provenance: str) -> list[dict]:
        # Group by table
        table_groups = defaultdict(list)
        for table, reference in references_in(provenance):
            table_groups[table].append(reference)

        results = []

        # Query each table for the relevant rows
        for table, references in table_groups.items():
            query = SQL("SELECT * FROM {table} WHERE {column} = ANY(%s)").format(
                table=Identifier(table), column=Identifier(REFERENCE_COLUMN)
            )
            cursor = await self._conn.cursor(row_factory=dict_row).execute(
                query, (references,)
            )
            data_by_reference = {r[REFERENCE_COLUMN]: r for r in await cursor.fetchall()}

            for reference in references:
                if row := data_by_reference.get(reference):
                    row = dict(row)
                    row.pop(REFERENCE_COLUMN)
                    # Belt and braces: this query runs with ProvSQL paused, so
                    # there should be no provenance column to strip.
                    row.pop("provsql", None)

                    results.append({"reference": reference, "data": row})
                else:
                    logger.warning(
                        "No data found for %s with reference %s", table, reference
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


def _aggregated_rows(node: dict) -> list[int]:
    """The actual rows going into each aggregate of an ``EXPLAIN ANALYZE`` plan."""
    found = []
    if "Aggregate" in node["Node Type"]:
        # Its input, not the subqueries of its expressions (InitPlan, SubPlan)
        found.append(sum(
            int(child.get("Actual Rows", 0) * child.get("Actual Loops", 1))
            for child in node.get("Plans", [])
            if child.get("Parent Relationship") == "Outer"
        ))
    for child in node.get("Plans", []):
        found.extend(_aggregated_rows(child))
    return found
