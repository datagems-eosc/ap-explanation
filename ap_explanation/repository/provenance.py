from collections import defaultdict
from contextlib import AbstractAsyncContextManager, nullcontext
from dataclasses import dataclass
from logging import getLogger
from typing import LiteralString, cast

from orjson import dumps
from psycopg import AsyncConnection, Rollback, errors
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg.types.json import set_json_dumps

from ap_explanation.errors import (
    InvalidProbabilityColumnError,
    ProvSqlInternalError,
    ProvSqlMissingError,
    QueryLimitExceededError,
)
from ap_explanation.internal.distributed_lock import RedisRowBudget
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repository.mapping.key_mapping import REFERENCE_COLUMN
from ap_explanation.semirings import semirings as all_semirings
from ap_explanation.types.provenance import Derivation, SemiringProvenance
from ap_explanation.types.semiring import DbSemiring

logger = getLogger(__name__)
set_json_dumps(dumps)


@dataclass(frozen=True)
class QueryLimits:
    """
    Settings bounding the resources of a provenance query, in PostgreSQL's
    syntax ('10min', '1GB'). ``None`` leaves the server's value.

    Without them a costly query runs until it finishes or the database dies,
    holding the per-database lock all along.
    """

    #: The one guard that holds whatever process grows: ProvSQL >= 1.13
    #: honours cancellation.
    statement_timeout: str | None = "10min"
    #: Memory ProvSQL's evaluations may add to the backend. It does not
    #: cover ProvSQL's store worker, hence the timeout.
    provsql_max_memory: str | None = "1GB"
    #: PostgreSQL's default is no limit, so a query can fill the disk.
    #: Superuser-only (or GRANT SET): skipped with a warning otherwise.
    temp_file_limit: str | None = "5GB"
    #: Parallel workers grow outside provsql.max_memory's budget.
    max_parallel_workers_per_gather: str | None = "0"
    #: Most rows the aggregates of the queries running at once on a server
    #: may receive. The circuit of an aggregate holds a gate per row, built
    #: while the query runs, before provsql.max_memory applies: past 16
    #: million rows on a 3 GB server, ProvSQL's store worker is killed and the
    #: whole server restarts, and the queries of all its databases add up.
    #: Counted by running the plain query first (see
    #: ProvenanceRepository._check_aggregated_rows); None skips the check.
    max_aggregated_rows: int | None = 8_000_000
    #: Rows each query reserves at least, whatever its aggregates: the
    #: evaluations of queries running at once add up in the store worker too.
    #: None is an eighth of max_aggregated_rows: at most eight queries at once,
    #: as many as were measured together on a 3 GB server.
    min_reserved_rows: int | None = None
    #: statement_timeout of that plain run. A plain plan can be much slower
    #: than ProvSQL's rewritten one: a question of the meteo use case whose
    #: provenance query takes 0.3 s was refused at 60 s, and explained in
    #: 157 s with 300 s.
    precheck_timeout: str = "180s"
    #: Most rows of an aggregate's formula to read (sr_formula_rows, ProvSQL
    #: 1.14): the first is the aggregate with its number of terms, each other
    #: a term of a few dozen symbols.
    max_formula_rows: int = 100

    def settings(self) -> dict[str, str]:
        """The settings to apply, keyed by PostgreSQL parameter name."""
        return {
            name: value
            for name, value in {
                "statement_timeout": self.statement_timeout,
                "provsql.max_memory": self.provsql_max_memory,
                "temp_file_limit": self.temp_file_limit,
                "max_parallel_workers_per_gather": self.max_parallel_workers_per_gather,
            }.items()
            if value
        }


class ProvenanceRepository:
    """
    Repository for all provenance-related operations.

    Handles both provenance setup (annotations) and querying with provenance tracking.
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
    ) -> ProvenanceRepository:
        repo = cls(conn, sql_rewriter, limits, budget)
        # Enabled per connection, never server-wide: the server default stays
        # provsql.active = 0 (set in the image's postgresql.conf) so that other
        # services querying the same database are never subject to ProvSQL's
        # rewriter, which does not support every SQL construct.
        await conn.execute("SET provsql.active = 1")
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
            await self._set_search_path(schema_name)
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
                                data=await self._fetch_related_data(expression, semiring),
                                truncated=truncated,
                            )
                        elif semiring.retrieval_function in row:
                            # NULL for an aggregate over no row
                            expression = row[semiring.retrieval_function] or ""
                            provenance[semiring.name] = SemiringProvenance(
                                expression=expression,
                                data=await self._fetch_related_data(expression, semiring),
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
            from ap_explanation.errors import TableNotAnnotatedError

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
                    "provsql.max_memory", self._limits.provsql_max_memory, str(e)
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
            (token, semiring.mapping_table, limit + 1),
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
        gate by ``replace_input`` instead, and ProvSQL's provenance_guard
        carries the tuple's mapping values over to it. Only these tuples are
        replaced, as the mappings keep the values of the tokens replaced.

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
                f"ProvSQL extension is not installed or not available: {e!s}"
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

        The mapping table is shared by every semiring using the same mapping
        strategy, as its contents only depend on the strategy: enabling a
        second semiring on a table only adds the table to that semiring's
        union view. A table annotated by an earlier version, which kept one
        identical mapping table per semiring, is migrated: one of its current
        per-semiring tables is renamed to the shared name and the others are
        dropped. Tables of the schema not annotated again keep their
        per-semiring tables, which the union views still read.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.
        Returns:
            True if the mapping table was newly created (or rebuilt from an
            outdated format), False if it was already in place.
        """
        prov_table = semiring.get_provenance_table_name_for(table_name)
        strategy = semiring.mappingStrategy
        legacy_tables = [
            s.get_legacy_provenance_table_name_for(table_name)
            for s in all_semirings
            if s.mappingStrategy.name == strategy.name
        ]

        await self._set_search_path(schema_name)

        # ProvSQL's create_provenance_mapping is idempotent (1.9.0+), but its
        # NOTICE-and-no-op path is indistinguishable from a real creation, so
        # the existence check is what computes this method's return value.
        existing = await self._existing_tables(schema_name, [prov_table, *legacy_tables])
        legacy_tables = [t for t in legacy_tables if t in existing]

        if prov_table in existing and not legacy_tables and await self._mapping_is_current(
            schema_name, table_name, prov_table, strategy
        ):
            logger.info(
                f"Provenance table '{prov_table}' already exists, skipping creation"
            )
            async with self._conn.transaction():
                await self._rebuild_union_mapping(schema_name, semiring)
            return False

        # Dropping a mapping table drops (CASCADE) the union views over it, of
        # every semiring sharing it: note them now to rebuild them all.
        enabled = await self._enabled_semirings(schema_name)

        if prov_table in existing and not await self._mapping_is_current(
            schema_name, table_name, prov_table, strategy
        ):
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
            existing.discard(prov_table)

        if prov_table not in existing:
            for legacy_table in legacy_tables:
                if await self._mapping_is_current(
                    schema_name, table_name, legacy_table, strategy
                ):
                    # The registry keys on the mapping's oid, which a rename
                    # keeps: the mapping stays maintained.
                    logger.info(f"Migrating provenance table '{legacy_table}' to '{prov_table}'")
                    async with self._conn.transaction():
                        await self._conn.execute(
                            SQL("ALTER TABLE {} RENAME TO {}").format(
                                Identifier(legacy_table), Identifier(prov_table)
                            )
                        )
                    legacy_tables.remove(legacy_table)
                    existing.add(prov_table)
                    break

        # Identical copies of the shared table, or outdated ones
        async with self._conn.transaction():
            for legacy_table in legacy_tables:
                logger.info(f"Dropping per-semiring provenance table '{legacy_table}'")
                await self._conn.execute(
                    SQL("DROP TABLE {} CASCADE").format(Identifier(legacy_table))
                )

        if prov_table in existing:
            async with self._conn.transaction():
                await self._rebuild_union_mappings(schema_name, [semiring, *enabled])
            return False

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

        # Rebuild the union mapping view for this semiring, and the ones the
        # drops above took with them
        async with self._conn.transaction():
            await self._rebuild_union_mappings(schema_name, [semiring, *enabled])

        return True

    async def _existing_tables(self, schema_name: str, names: list[str]) -> set[str]:
        """The tables of *names* that exist in the schema."""
        async with self._conn.transaction():
            cursor = await self._conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s AND tablename = ANY(%s)",
                (schema_name, names),
            )
            return {row[0] for row in await cursor.fetchall()}

    async def _enabled_semirings(self, schema_name: str) -> list[DbSemiring]:
        """The semirings whose union mapping exists in the schema."""
        async with self._conn.transaction():
            cursor = await self._conn.execute(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                " WHERE n.nspname = %s AND c.relname = ANY(%s)",
                (schema_name, [s.union_table_name for s in all_semirings]),
            )
            names = {row[0] for row in await cursor.fetchall()}
        return [s for s in all_semirings if s.union_table_name in names]

    async def _rebuild_union_mappings(
        self, schema_name: str, semirings: list[DbSemiring]
    ) -> None:
        """Rebuild the union mapping of each semiring, once."""
        rebuilt = set()
        for semiring in semirings:
            if semiring.name not in rebuilt:
                rebuilt.add(semiring.name)
                await self._rebuild_union_mapping(schema_name, semiring)

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

        The mapping table is shared by every semiring using the same mapping
        strategy, so this removes the table from all of them.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.
        Returns:
            True if the semiring's provenance table existed and was dropped, False otherwise.

        """
        prov_tables = [
            semiring.get_provenance_table_name_for(table_name),
            semiring.get_legacy_provenance_table_name_for(table_name),
        ]

        await self._set_search_path(schema_name)

        # Check if the provenance tables exist before attempting to drop them
        existing = await self._existing_tables(schema_name, prov_tables)
        if not existing:
            return False

        # The drops take the union views over the tables with them
        enabled = await self._enabled_semirings(schema_name)
        async with self._conn.transaction():
            for prov_table in existing:
                await self._conn.execute(
                    SQL("DROP TABLE {} CASCADE").format(Identifier(prov_table))
                )
            await self._rebuild_union_mappings(schema_name, [semiring, *enabled])

        return True

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

        It reads the mapping tables shared by the semiring's mapping strategy,
        and the semiring's own tables that earlier versions created, for the
        tables not migrated yet.
        """
        await self._set_search_path(schema_name)

        def ending_with(suffix: str) -> str:
            # LIKE pattern; _ is a LIKE wildcard
            return "%" + suffix.replace("_", "\\_")

        cursor = await self._conn.execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s"
            " AND (tablename LIKE %s OR tablename LIKE %s)",
            (
                schema_name,
                ending_with(semiring.table_suffix),
                ending_with(semiring.legacy_table_suffix),
            ),
        )
        names = {row[0] for row in await cursor.fetchall()}
        shared = {n for n in names if n.endswith(semiring.table_suffix)}
        legacy = {
            n
            for n in names - shared
            # A table with both would have each of its tokens twice
            if n.removesuffix(semiring.legacy_table_suffix) + semiring.table_suffix not in shared
        }
        mapping_tables = sorted(shared | legacy)

        if not mapping_tables:
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
                f"SELECT value, provenance FROM {schema_name}.{table}"
                for table in mapping_tables
            ]
        )
        composed_rq = SQL("CREATE VIEW {} AS {}").format(
            qualified_name, SQL(cast(LiteralString, union_query))
        )
        await self._conn.execute(composed_rq)

        logger.info(
            f"Created {schema_name}.{name} view from {len(mapping_tables)} mapping tables"
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
