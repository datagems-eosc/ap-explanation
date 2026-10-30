from logging import getLogger
from typing import LiteralString, cast

from psycopg import AsyncConnection, errors
from psycopg.sql import SQL, Identifier

from ap_explanation.domains.semiring import MAPPING_VIEW
from ap_explanation.errors import (
    InvalidProbabilityColumnError,
    ProvSqlInternalError,
    ProvSqlMissingError,
    TableOrSchemaNotFoundError,
)
from ap_explanation.repositories.legacy_mappings import (
    LEGACY_SUFFIXES,
    drop_union_relations,
    legacy_tables_of,
    replace_legacy_tables,
)
from ap_explanation.repositories.provsql import (
    MAPPING_TABLE_SUFFIX,
    REFERENCE_COLUMN,
    reference_default,
    set_search_path,
)

logger = getLogger(__name__)


class AnnotationRepository:
    """
    Prepares tables for provenance queries: annotations, mappings and tuple
    probabilities. None of it depends on the semiring: a semiring is only
    chosen when a query evaluates it.
    """

    _conn: AsyncConnection

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

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
                await set_search_path(self._conn, schema_name)
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
                await set_search_path(self._conn, schema_name)
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

        await set_search_path(self._conn, schema_name)

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
        await set_search_path(self._conn, schema_name)

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

    async def disable(self, schema_name: str, table_name: str) -> None:
        """
        Undo :meth:`enable` for a table: its provenance, its reference column
        and its mapping tables.

        Args:
            schema_name: The schema where the base table is located.
            table_name: The name of the base table.
        """

        await set_search_path(self._conn, schema_name)

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
