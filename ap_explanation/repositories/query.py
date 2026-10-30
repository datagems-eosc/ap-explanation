from collections import defaultdict
from logging import getLogger
from typing import LiteralString, cast

from orjson import dumps
from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg.types.json import set_json_dumps

from ap_explanation.domains.provenance import Derivation, SemiringProvenance
from ap_explanation.domains.semiring import MAPPING_VIEW, DbSemiring
from ap_explanation.errors import (
    ProvSqlInternalError,
    QueryLimitExceededError,
    TableNotAnnotatedError,
)
from ap_explanation.internal.distributed_lock import RedisRowBudget
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repositories.provsql import (
    REFERENCE_COLUMN,
    references_in,
    set_search_path,
)
from ap_explanation.repositories.query_guard import QueryGuard
from ap_explanation.settings import QueryLimits

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
        self._guard = QueryGuard(conn, self._limits, budget)

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
                :class:`QueryLimits`
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
                await self._guard.apply_limits()
                aggregated_rows = await self._guard.check_aggregated_rows(query)

            # Waiting for the server's budget outside a transaction: an idle one
            # would hold back VACUUM, or be ended by idle_in_transaction_session_timeout
            async with self._guard.reserve(aggregated_rows), self._conn.transaction():
                await self._guard.apply_limits()

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
            # The mapping view doesn't exist, meaning no table of the schema has been annotated
            logger.warning(f"Table not annotated: {e}")
            raise TableNotAnnotatedError(schema_name=schema_name) from e
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
            (token, MAPPING_VIEW, limit + 1),
        )
        rows = await cursor.fetchall()
        if len(rows) == 1:
            return rows[0][1], False
        return "\n".join(f"{name} = {formula}" for name, formula in rows[:limit]), len(rows) > limit

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
