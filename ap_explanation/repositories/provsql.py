"""
ProvSQL, as this service uses it, and how the rows of an annotated table are
labelled.

Each row carries a surrogate reference, a value rather than its physical
location (the CTID this replaced), so it survives UPDATE, VACUUM FULL and
ProvSQL's own data-modification rewrites.

The reference lives in a plain column with a DEFAULT (rather than a GENERATED
column) for two reasons: ProvSQL's ``provenance_guard`` trigger reads it as
``NEW.provsql_ref`` in a BEFORE INSERT trigger, where a generated column is not
yet computed, and a DEFAULT expression cannot reference other columns anyway —
which rules out deriving the reference from a primary key, and lets tables
without one work the same way.
"""

import asyncio
import time
from collections import defaultdict
from logging import getLogger
from re import compile
from typing import LiteralString, cast

from orjson import dumps
from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg.types.json import set_json_dumps

from ap_explanation.domains.provenance import Derivation, SemiringProvenance
from ap_explanation.domains.semiring import MAPPING_VIEW, DbSemiring
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
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repositories.legacy_mappings import (
    LEGACY_SUFFIXES,
    drop_union_relations,
    legacy_tables_of,
    replace_legacy_tables,
)
from ap_explanation.repositories.query_guard import QueryGuard
from ap_explanation.settings import QueryLimits

logger = getLogger(__name__)
set_json_dumps(dumps)

# Name of the column added to every annotated table to hold its stable row
# reference. It is also the mapping attribute handed to
# create_provenance_mapping, which is what makes maintained mappings possible:
# provenance_guard interpolates the attribute as ($1).%I, so it has to name a
# real column rather than an expression.
REFERENCE_COLUMN = "provsql_ref"

# Suffix of a table's mapping table, shared by all semirings: the reference
# identifies the row, not the provenance flavour.
MAPPING_TABLE_SUFFIX = "_provkey"

_REFERENCE_RE = compile(r"([a-zA-Z_][a-zA-Z0-9_]*)@k([0-9a-f]+)")


def reference_default(table_name: str) -> str:
    """
    SQL DEFAULT expression populating the reference column.

    A random suffix rather than a key-derived one: the expression must be
    self-contained, and the hex alphabet keeps the reference unambiguous
    inside the ``{…}`` / ``,`` / ``⊗`` / ``⊕`` syntax the semirings emit.
    """
    literal = table_name.replace("'", "''")
    return f"'{literal}@k' || translate(gen_random_uuid()::text, '-', '')"


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
            True if the table was newly annotated or its mapping table created
            (or rebuilt from an outdated format), False if both were in place.
        """
        newly_annotated = await self._add_provenance(schema_name, table_name)
        newly_mapped = await self._ensure_mapping(schema_name, table_name)
        return newly_annotated or newly_mapped

    async def disable(self, schema_name: str, table_name: str) -> None:
        """
        Undo :meth:`enable` for a table: its provenance, its reference column
        and its mapping tables.

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

        prov_tables = await self._existing_tables(
            schema_name, [f"{table_name}{MAPPING_TABLE_SUFFIX}", *legacy_tables_of(table_name)]
        )
        if not prov_tables:
            return

        # The drops take the mapping view with them
        async with self._conn.transaction():
            for prov_table in prov_tables:
                await self._conn.execute(
                    SQL("DROP TABLE {} CASCADE").format(Identifier(prov_table))
                )
            await self._rebuild_mapping_view(schema_name)

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
            REFERENCE_COLUMN,
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

    async def _ensure_mapping(self, schema_name: str, table_name: str) -> bool:
        """
        Give a table its mapping table, and add it to the mapping view.

        The mapping table does not depend on the semiring. A table annotated by
        an earlier version, which kept one identical mapping table per
        semiring, is migrated: one of its current per-semiring tables is
        renamed to the shared name and the others are dropped.

        Returns:
            True if the mapping table was newly created (or rebuilt from an
            outdated format), False if it was already in place.
        """
        prov_table = f"{table_name}{MAPPING_TABLE_SUFFIX}"
        legacy_tables = legacy_tables_of(table_name)

        await self._set_search_path(schema_name)

        # ProvSQL's create_provenance_mapping is idempotent (1.9.0+), but its
        # NOTICE-and-no-op path is indistinguishable from a real creation, so
        # the existence check is what computes this method's return value.
        existing = await self._existing_tables(schema_name, [prov_table, *legacy_tables])
        legacy_tables = [t for t in legacy_tables if t in existing]
        has_mapping = prov_table in existing

        if has_mapping and not await self._mapping_is_current(
            schema_name, table_name, prov_table
        ):
            # Built by an earlier version (a ctid mapping, or one predating
            # maintained registration): its stored values cannot be converted,
            # so rebuild from the tokens the source table currently holds.
            logger.info(
                f"Provenance table '{prov_table}' is not a current maintained mapping, rebuilding"
            )
            async with self._conn.transaction():
                await self._conn.execute(
                    SQL("DROP TABLE {} CASCADE").format(Identifier(prov_table))
                )
            has_mapping = False

        if legacy_tables:
            current = [
                t for t in legacy_tables
                if await self._mapping_is_current(schema_name, table_name, t)
            ]
            keep = current[0] if current and not has_mapping else None
            await replace_legacy_tables(self._conn, legacy_tables, prov_table, keep)
            has_mapping = has_mapping or keep is not None

        if not has_mapping:
            await self._ensure_reference_column(schema_name, table_name)

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
                        (prov_table, table_name, REFERENCE_COLUMN),
                    )
            except Exception as e:
                logger.error(f"Unexpected error in create_provenance_mapping: {e}")
                raise

        async with self._conn.transaction():
            await self._rebuild_mapping_view(schema_name)

        return not has_mapping

    async def _existing_tables(self, schema_name: str, names: list[str]) -> set[str]:
        """The tables of *names* that exist in the schema."""
        async with self._conn.transaction():
            cursor = await self._conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s AND tablename = ANY(%s)",
                (schema_name, names),
            )
            return {row[0] for row in await cursor.fetchall()}

    async def _mapping_is_current(
        self, schema_name: str, table_name: str, prov_table: str
    ) -> bool:
        """
        Check whether an existing mapping table has the current layout, and so
        can be left alone.

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
                    REFERENCE_COLUMN,
                    f"{schema_name}.{prov_table}",
                ),
            )
            has_reference_column, is_registered = await cursor.fetchone()

        return bool(has_reference_column and is_registered)

    async def _ensure_reference_column(self, schema_name: str, table_name: str) -> None:
        """Add the reference column to a source table."""
        await self._set_search_path(schema_name)

        async with self._conn.transaction():
            await self._conn.execute(
                SQL(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} text NOT NULL DEFAULT ({})"
                ).format(
                    Identifier(table_name),
                    Identifier(REFERENCE_COLUMN),
                    SQL(cast(LiteralString, reference_default(table_name))),
                )
            )
            # Deliberately not UNIQUE: ProvSQL's data-modification machinery
            # re-inserts rows carrying their existing column values, which a
            # unique index could reject.
            await self._conn.execute(
                SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                    Identifier(f"{table_name}_{REFERENCE_COLUMN}_idx"),
                    Identifier(table_name),
                    Identifier(REFERENCE_COLUMN),
                )
            )

    async def _rebuild_mapping_view(self, schema_name: str) -> None:
        """
        Build or rebuild the view covering every mapping table of the schema.

        The semiring retrieval functions (``sr_why(provenance(), 'provkey_mapping')``)
        take a single regclass, so a query spanning several tables needs one
        relation holding all of their tokens. It cannot be a single shared
        mapping table instead: ``provenance_mapping_registry`` keys on the
        mapping's oid, so one mapping table can only ever serve one source
        table.

        It is a view rather than the materialised snapshot this used to build,
        so that the rows ``provenance_guard`` appends to a maintained mapping
        are visible to queries without re-annotating.
        """
        await drop_union_relations(self._conn, schema_name)

        mapping_tables = await self._mapping_tables(schema_name)
        if not mapping_tables:
            logger.warning(
                f"No tables ending with {MAPPING_TABLE_SUFFIX} found in schema {schema_name}"
            )
            return

        # UNION ALL, not UNION: provenance tokens are unique per input row, so
        # deduplicating is pure cost and it stops the planner pushing the
        # evaluator's token join down into each mapping table's index.
        union_query = SQL(" UNION ALL ").join(
            SQL("SELECT value, provenance FROM {}.{}").format(Identifier(schema_name), Identifier(table))
            for table in mapping_tables
        )
        await self._conn.execute(
            SQL("CREATE OR REPLACE VIEW {}.{} AS {}").format(
                Identifier(schema_name), Identifier(MAPPING_VIEW), union_query
            )
        )

        logger.info(
            f"Created {schema_name}.{MAPPING_VIEW} view from {len(mapping_tables)} mapping tables"
        )

    async def _mapping_tables(self, schema_name: str) -> list[str]:
        """
        One mapping table per annotated table of the schema: the shared one or,
        for a table not migrated yet, one of its per-semiring tables, which hold
        the same rows.
        """

        def ending_with(suffix: str) -> str:
            # LIKE pattern; _ is a LIKE wildcard
            return "%" + suffix.replace("_", "\\_")

        suffixes = [MAPPING_TABLE_SUFFIX, *LEGACY_SUFFIXES]
        cursor = await self._conn.execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s AND tablename LIKE ANY(%s)",
            (schema_name, [ending_with(suffix) for suffix in suffixes]),
        )
        names = [row[0] for row in await cursor.fetchall()]

        # The shared table first: a table with both would have each of its tokens twice
        by_table: dict[str, str] = {}
        for suffix in suffixes:
            for name in names:
                if name.endswith(suffix):
                    by_table.setdefault(name.removesuffix(suffix), name)
        return sorted(by_table.values())

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
