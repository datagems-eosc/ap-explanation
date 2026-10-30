from contextlib import AbstractAsyncContextManager, nullcontext
from logging import getLogger
from typing import LiteralString, cast

from psycopg import AsyncConnection, Rollback, errors
from psycopg.sql import SQL

from ap_explanation.errors import QueryLimitExceededError
from ap_explanation.internal.distributed_lock import RedisRowBudget
from ap_explanation.settings import QueryLimits

logger = getLogger(__name__)


class QueryGuard:
    """
    Keeps a provenance query from harming its server: the limits of its
    session, the rows of its aggregates, and their share of the server's budget.
    """

    def __init__(
        self,
        conn: AsyncConnection,
        limits: QueryLimits,
        budget: RedisRowBudget | None = None,
    ):
        self._conn = conn
        self._limits = limits
        self._budget = budget

    async def check_aggregated_rows(self, query: str) -> int:
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

    def reserve(self, aggregated_rows: int) -> AbstractAsyncContextManager[None]:
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

    async def apply_limits(self) -> None:
        """
        Apply the :class:`QueryLimits` to the current transaction.

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
