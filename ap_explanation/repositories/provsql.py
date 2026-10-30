"""
ProvSQL, as this service uses it, and how the rows of an annotated table are
labelled.

A row is labelled by its provsql token, so it needs no column of its own and
survives UPDATE and VACUUM FULL. A token only changes with the row's
probability (:meth:`ProvSqlRepository.set_probabilities`), or with
``provsql.update_provenance`` on, which nothing here sets.
"""

import asyncio
import time
from collections import defaultdict
from logging import getLogger
from re import compile
from typing import LiteralString, cast
from uuid import UUID

from orjson import dumps
from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier, Literal
from psycopg.types.json import set_json_dumps

from ap_explanation.domains.provenance.derivation import Derivation, SemiringProvenance
from ap_explanation.domains.provenance.semiring import MAPPING_VIEW, DbSemiring
from ap_explanation.domains.provenance.sql_rewriter import SqlRewriter
from ap_explanation.errors import (
    DatabaseInUseError,
    InvalidProbabilityColumnError,
    ProvSqlInternalError,
    ProvSqlMissingError,
    QueryLimitExceededError,
    TableNotAnnotatedError,
    TableOrSchemaNotFoundError,
)
from ap_explanation.internal.distributed_lock import RedisRowBudget
from ap_explanation.repositories.legacy_mappings import remove_legacy_mappings
from ap_explanation.repositories.query_guard import QueryGuard
from ap_explanation.settings import QueryLimits

logger = getLogger(__name__)
set_json_dumps(dumps)

# All 32 digits of a token: a formula also holds values, like an e-mail address
# of a MIN(email) (anna@kfe.org), which are not references
_REFERENCE_RE = compile(r"([a-zA-Z_][a-zA-Z0-9_]*)@k([0-9a-f]{32})")


def references_in(expression: str) -> list[tuple[str, str]]:
    """
    The ``(table, reference)`` pairs a provenance expression names.

    Args:
        expression: Supports both brace-wrapped format (formula semiring):
                      {{table@k<hex>}⊗{table@k<hex>}}
                    and comma-separated format (why-provenance semiring):
                      {"{table@k<hex>,table@k<hex>,...}"}
    """
    return [(table, f"{table}@k{key}") for table, key in _REFERENCE_RE.findall(expression)]


def token_of(reference: str) -> UUID:
    """The provsql token of the row a reference names."""
    return UUID(reference.rpartition("@k")[2])


class ProvSqlRepository:
    """
    What this service does with ProvSQL on one database: prepare tables, compute
    the provenance of queries, and reclaim the circuit store.

    It owns the connection's session, as search_path and provsql.active are per
    session.
    """

    _conn: AsyncConnection
    _sql_rewriter: SqlRewriter
    _limits: QueryLimits
    #: Whether ProvSQL writes formulas as rows (sr_formula_rows, ProvSQL 1.14)
    _formula_rows: bool = False

    #: Where an aggregate query's result is kept, for the rows of its formulas
    #: to be read from its tokens, valid in this transaction only
    RESULT_TABLE = "apx_result"

    #: Seconds between two attempts at reclaiming the circuit store
    _CLEANUP_RETRY_INTERVAL = 2

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
    ) -> ProvSqlRepository:
        repo = cls(conn, sql_rewriter, limits, budget)
        # Enabled per connection, never server-wide: the server default stays
        # provsql.active = 0 (set in the image's postgresql.conf) so that other
        # services querying the same database are never subject to ProvSQL's
        # rewriter, which does not support every SQL construct.
        await conn.execute("SET provsql.active = 1")
        cursor = await conn.execute("SELECT to_regproc('provsql.sr_formula_rows') IS NOT NULL")
        repo._formula_rows = (await cursor.fetchone())[0]
        return repo

    async def enable(self, schema_name: str, table_name: str) -> bool:
        """
        Make a table ready for provenance queries, in every semiring.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.
        Returns:
            True if the table was newly annotated, False if it already was.
        """
        newly_annotated = await self._add_provenance(schema_name, table_name)
        await remove_legacy_mappings(self._conn, schema_name, table_name)
        return newly_annotated

    async def disable(self, schema_name: str, table_name: str) -> None:
        """
        Undo :meth:`enable` for a table.

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

        await remove_legacy_mappings(self._conn, schema_name, table_name)

    async def set_probabilities(
        self, schema_name: str, table_name: str, column: str | None
    ) -> None:
        """
        Set the probability of every tuple of a provenance-annotated table.

        ProvSQL keeps probabilities in its circuit store, not in the table, so
        they outlive the request that set them. Every tuple is therefore set on
        each call: to its value in *column*, or to 1 (certain) when *column* is
        ``None`` or the value is NULL, so that nothing an earlier request set
        lingers. ``set_prob`` does not check its range, hence the validation
        beforehand.

        ProvSQL writes a probability once: ``set_prob`` refuses to change one
        already set. A tuple whose probability changes is given a fresh input
        gate by ``replace_input`` instead, which also gives it a new reference.
        Only these tuples are replaced, so the others keep theirs.

        Args:
            schema_name: The schema where the table is located.
            table_name: The annotated table.
            column: The column holding the probabilities, if any.

        Raises:
            InvalidProbabilityColumnError: If the column does not exist, is not
                numeric, or holds a value outside [0, 1].
        """
        table = Identifier(table_name)
        probability = (
            SQL("COALESCE({}::float8, 1.0)").format(Identifier(column))
            if column
            else SQL("1.0")
        )

        try:
            async with self._conn.transaction():
                await self._set_search_path(schema_name)
                # The provsql column is read directly, so ProvSQL's rewriter
                # has nothing to add. SET LOCAL reverts at commit.
                await self._conn.execute("SET LOCAL provsql.active = 0")

                if column:
                    cursor = await self._conn.execute(
                        SQL("SELECT count(*) FROM {} WHERE {} NOT BETWEEN 0 AND 1").format(
                            table, probability
                        )
                    )
                    (out_of_range,) = await cursor.fetchone()
                    if out_of_range:
                        raise InvalidProbabilityColumnError(
                            table_name,
                            column,
                            f"{out_of_range} value(s) outside [0, 1]",
                        )

                # Counted rather than fetched, to avoid shipping back one
                # void value per tuple.
                await self._conn.execute(
                    SQL(
                        "SELECT count(*) FROM (SELECT set_prob(provsql, {}) FROM {}"
                        " WHERE NOT probability_is_set(provsql)) AS s"
                    ).format(probability, table)
                )
                await self._conn.execute(
                    SQL(
                        "UPDATE {} SET provsql = replace_input(provsql, {})"
                        " WHERE get_prob(provsql) IS DISTINCT FROM {}"
                    ).format(table, probability, probability)
                )
        except (
            errors.UndefinedColumn,
            errors.InvalidTextRepresentation,
            errors.CannotCoerce,
        ) as e:
            # Only a column we were asked to read can be at fault here
            raise InvalidProbabilityColumnError(
                table_name, column or "", str(e)
            ) from e
        except errors.InternalError_ as e:
            logger.error(
                f"ProvSQL internal error while setting probabilities on '{table_name}': {e}"
            )
            raise ProvSqlInternalError(
                f"ProvSQL internal error occurred while setting probabilities on '{table_name}'. "
                f"Error details: {e!s}"
            ) from e

    async def compute(
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
            SqlRewriter.PROBABILITY_FUNCTION,
            *(s.retrieval_function for s in semirings),
            *(c for s in rows_semirings for c in (s.rows_function, f"{s.rows_function}_token")),
        }

        try:
            await self._set_search_path(schema_name)
            async with self._conn.transaction():
                await self._guard.apply_limits()
                aggregated_rows = await self._guard.check_aggregated_rows(query)

            # Waiting for the server's budget outside a transaction: an idle one
            # would hold back VACUUM, or be ended by idle_in_transaction_session_timeout
            async with self._guard.reserve(aggregated_rows), self._conn.transaction():
                await self._guard.apply_limits()
                await self._create_mapping_view(schema_name)

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

                # Not left on the connection: it would stop the other sessions
                # dropping the tables it reads
                await self._conn.execute(SQL("DROP VIEW pg_temp.{}").format(Identifier(MAPPING_VIEW)))

            return results
        except errors.UndefinedTable as e:
            # A table of the query that does not exist, and so is not annotated
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

    async def cleanup_circuits(self, wait: float) -> dict:
        """
        Reclaim ProvSQL's circuit store: ``provsql.circuit_cleanup()`` keeps the
        gates a token stored in the database still references and rewrites its
        files.

        Like ``DROP DATABASE``, it refuses to run while another session is
        connected to the database.

        Args:
            wait: Seconds to keep retrying while other sessions are connected.
                The connection has to be in autocommit.

        Returns:
            ProvSQL's counts before and after.

        Raises:
            DatabaseInUseError: If other sessions are still connected after *wait*
            ProvSqlMissingError: If ProvSQL has no ``circuit_cleanup()``
        """
        db_name = self._conn.info.dbname
        deadline = time.monotonic() + wait
        while True:
            try:
                # Must be the first statement of its transaction: autocommit
                cursor = await self._conn.cursor(row_factory=dict_row).execute(
                    "SELECT * FROM provsql.circuit_cleanup()"
                )
                return await cursor.fetchone() or {}
            except errors.ObjectInUse:
                if time.monotonic() >= deadline:
                    break
                await asyncio.sleep(self._CLEANUP_RETRY_INTERVAL)
            except (errors.UndefinedFunction, errors.InvalidSchemaName) as e:
                raise ProvSqlMissingError(
                    f"provsql.circuit_cleanup() is not available on '{db_name}' "
                    f"(ProvSQL >= 1.13 required): {e!s}"
                ) from e

        cursor = await self._conn.execute(
            "SELECT format('%s@%s (%s)', usename, coalesce(client_addr::text, 'local'),"
            " coalesce(nullif(application_name, ''), 'unnamed'))"
            " FROM pg_stat_activity WHERE datname = current_database()"
            " AND pid <> pg_backend_pid() AND backend_type = 'client backend'"
        )
        raise DatabaseInUseError(db_name, [row[0] for row in await cursor.fetchall()])

    async def _set_search_path(self, schema_name: str) -> None:
        """Set the PostgreSQL search path for the current connection."""
        await self._conn.execute(
            SQL("SET search_path TO {}, public, provsql;").format(Identifier(schema_name))
        )

    async def _add_provenance(self, schema_name: str, table_name: str) -> bool:
        """Add ProvSQL's provenance to a table, and return whether it was missing."""
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
                f"ProvSQL extension is not installed or not available: {e!s}"
            ) from e
        except errors.UndefinedTable as e:
            logger.warning(
                f"Table '{table_name}' does not exist in schema '{schema_name}': {e}"
            )
            raise TableOrSchemaNotFoundError(
                table_name=table_name, schema_name=schema_name
            ) from e

        return True

    async def _create_mapping_view(self, schema_name: str) -> None:
        """
        Create the view semiring calls read to label rows, for the current
        transaction.

        A semiring call (``sr_why(provenance(), 'apx_mapping')``) takes a single
        relation holding the tokens of every table of its query. Reading them
        from the tables themselves leaves no mapping to keep up to date. The
        one thing it misses is a row changed with ``provsql.update_provenance``
        on: its token is no longer the input gate its provenance reaches, and
        that gate loses its label.

        Raises:
            TableNotAnnotatedError: If no table of the schema is annotated
        """
        cursor = await self._conn.execute(
            "SELECT c.relname FROM pg_trigger t"
            " JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = %s AND t.tgname = 'provenance_guard'",
            (schema_name,),
        )
        tables = [row[0] for row in await cursor.fetchall()]
        if not tables:
            raise TableNotAnnotatedError(schema_name=schema_name)

        # shortcut: every annotated table of the schema, not those of the query,
        # which would miss tables read through a view. ProvSQL rewrites each
        # semiring lookup through this view, once per table: about 0.1 ms more
        # per call with 2 tables, 1 ms with 30. Narrow it if schemas get that big.
        #
        # The hex alphabet keeps a reference unambiguous inside the
        # ``{…}`` / ``,`` / ``⊗`` / ``⊕`` syntax the semirings emit. UNION ALL,
        # not UNION: a token belongs to one row, and deduplicating would stop
        # the planner pushing the lookup down into each table's provsql index.
        union_query = SQL(" UNION ALL ").join(
            SQL("SELECT {} || translate(provsql::text, '-', '') AS value, provsql AS provenance FROM {}.{}").format(
                Literal(f"{table}@k"), Identifier(schema_name), Identifier(table)
            )
            for table in tables
        )
        await self._conn.execute(
            SQL("CREATE TEMP VIEW {} AS {}").format(Identifier(MAPPING_VIEW), union_query)
        )

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
            # Aliased: ProvSQL hides a column named provsql from the results
            query = SQL("SELECT provsql AS apx_token, * FROM {} WHERE provsql = ANY(%s)").format(Identifier(table))
            cursor = await self._conn.cursor(row_factory=dict_row).execute(
                query, ([token_of(reference) for reference in references],)
            )
            data_by_token = {r.pop("apx_token"): r for r in await cursor.fetchall()}

            for reference in references:
                if row := data_by_token.get(token_of(reference)):
                    results.append({"reference": reference, "data": row})
                else:
                    logger.warning(
                        "No data found for %s with reference %s", table, reference
                    )

        return results
