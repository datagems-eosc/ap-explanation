from psycopg import AsyncConnection

from ap_explanation.errors import AggregateSemiringError, ProvSqlInternalError
from ap_explanation.internal.distributed_lock import RedisRowBudget
from ap_explanation.internal.explainer import Explainer
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repository.annotation import AnnotationRepository
from ap_explanation.repository.provsql import activate_provsql
from ap_explanation.repository.query import QueryRepository
from ap_explanation.semirings import semirings as all_semirings
from ap_explanation.settings import QueryLimits
from ap_explanation.types.provenance import (
    Derivation,
    Provenance,
)
from ap_explanation.types.provenance_analytical_pattern import (
    ProvenanceAnalyticalPattern,
)
from ap_explanation.types.semiring import DbSemiring


class ProvenanceService:
    """
    Service layer for provenance operations.

    Orchestrates business logic by delegating to the repositories.
    """

    _queries: QueryRepository
    _annotations: AnnotationRepository
    _explainer: Explainer

    def __init__(self, queries: QueryRepository, annotations: AnnotationRepository, explainer: Explainer):
        self._queries = queries
        self._annotations = annotations
        self._explainer = explainer

    @classmethod
    async def create(
        cls,
        conn: AsyncConnection,
        sql_rewriter: SqlRewriter,
        explainer: Explainer,
        limits: QueryLimits | None = None,
        budget: RedisRowBudget | None = None,
    ) -> ProvenanceService:
        """
        Build the service over *conn*. Its repositories have to share the
        connection: search_path and provsql.active are per session.
        """
        await activate_provsql(conn)
        queries = await QueryRepository.create(conn, sql_rewriter, limits, budget)
        return cls(queries, AnnotationRepository(conn), explainer)

    async def explain_ap(
        self,
        ap: ProvenanceAnalyticalPattern,
        semiring_name: str | None = None,
        compute_probability: bool = False,
    ) -> Provenance:
        """
        The whole lifecycle of an AP: annotate its tables, compute the
        provenance of its query, and explain it.

        If *semiring_name* is ``None`` all configured semirings are used; otherwise
        only the requested one. If *compute_probability* is set, tuple probabilities
        are read from each data node's ``probabilityColumn`` and every result row
        gets its probability.
        """
        if semiring_name is not None:
            semiring = next(
                (s for s in all_semirings if s.name == semiring_name), None)
            if not semiring:
                available = ", ".join(s.name for s in all_semirings)
                raise ValueError(
                    f"Semiring '{semiring_name}' not found. Available: {available}"
                )
            target_semirings = [semiring]
        else:
            target_semirings = all_semirings

        ds = ap.data_source
        query = ap.sql_operator.properties["query"]

        # 1. Annotate
        for table_name in ds.table_names:
            await self.annotate_dataset(table_name, ds.schema_name, target_semirings)

        # 1b. Set tuple probabilities (if requested)
        if compute_probability:
            await self.set_probabilities(ds.schema_name, ds.probability_columns)

        # 2. Compute provenance
        derivations = await self.compute_provenance(
            ds.schema_name, query, target_semirings, compute_probability)

        # 3. Compute NL explanation (if enabled)
        explanation = await self.explain(ds.schema_name, query, derivations)

        # # 4. Remove annotation (see provsql issue #67 workaround)
        # for table_name in ds.table_names:
        #     await self.remove_annotation(table_name, ds.schema_name)

        return Provenance(derivations=derivations, explanation=explanation)

    async def annotate_dataset(self, table_name: str, schema_name: str, semirings: list[DbSemiring]) -> bool:
        """
        Annotate a table with provenance information.

        Args:
            table_name: Name of the table to annotate
            schema_name: Schema where the table is located
            semirings: List of semirings to enable for the table

        Returns:
            bool: True if the table was annotated or semirings were newly enabled, False otherwise
        """
        newly_annotated = await self._annotations.enable_provenance(schema_name, table_name)

        semiring_newly_enabled = False
        for semiring in semirings:
            semiring_newly_enabled |= await self._annotations.add_semiring(schema_name, table_name, semiring)

        return newly_annotated or semiring_newly_enabled

    async def remove_annotation(self, table_name: str, schema_name: str) -> bool:
        """
        Remove provenance annotations from a table. This removes ALL provenance information for the table, including all semirings.

        Args:
            table_name: Name of the table to remove annotations from
            schema_name: Schema where the table is located

        Returns:
            bool: True if any semiring was removed, False if none were found
        """

        await self._annotations.remove_provenance(schema_name, table_name)

        # Note : This will be unused for the time being,
        # we don't support removing a single semiring, but we want to keep the option open to do so in the future, and it makes the implementation simpler for now to just remove all semirings when removing annotation
        any_removed = False
        for semiring in all_semirings:
            was_removed = await self._annotations.remove_semiring(schema_name, table_name, semiring)
            any_removed |= was_removed

        return True

    async def set_probabilities(self, schema_name: str, probability_columns: dict[str, str | None]) -> None:
        """
        Set the tuple probabilities of annotated tables.

        Args:
            schema_name: Schema where the tables are located
            probability_columns: Maps each table to the column holding its tuples'
                probabilities, or to None when all its tuples are certain
        """
        for table_name, column in probability_columns.items():
            await self._annotations.set_probabilities(schema_name, table_name, column)

    async def compute_provenance(self, schema_name: str, sql_query: str, semirings: list[DbSemiring], compute_probability: bool = False) -> list[Derivation]:
        """
        Execute a SQL query with provenance tracking in every semiring, and the
        probability if requested, all in one query.

        Each returned :class:`~ap_explanation.types.provenance.Derivation`
        contains the original query columns in ``answer`` and a per-semiring
        mapping in ``provenance``. On an aggregate query, or one whose rows'
        provenance involves an aggregate below the top level, that mapping holds
        only the semirings that support aggregates (formula).

        Args:
            schema_name: Schema where the query should be executed
            sql_query: The SQL query to execute with provenance
            semirings: List of semiring configurations to compute
            compute_probability: Also evaluate each row's probability. Tuple
                probabilities must have been set with :meth:`set_probabilities`

        Returns:
            List of :class:`~ap_explanation.types.provenance.Derivation` instances,
            one per result row, ordered as returned by the database.

        Raises:
            AggregateSemiringError: If the query aggregates and none of the
                semirings supports aggregates
        """
        try:
            return await self._queries.query(schema_name, sql_query, semirings, compute_probability)
        except ProvSqlInternalError as e:
            # A comparison on an aggregate below the top level (a CTE's SUM in WHERE)
            # can reach a row's provenance, which the rewriting does not see: only
            # the semirings supporting aggregates evaluate it
            if "does not support value gates" not in str(e):
                raise
            aggregate_semirings = [s for s in semirings if s.supports_aggregates]
            if not aggregate_semirings:
                raise AggregateSemiringError([s.name for s in semirings]) from e
            if aggregate_semirings == semirings:
                raise
            return await self._queries.query(schema_name, sql_query, aggregate_semirings, compute_probability)

    async def explain(self, schema_name: str, sql_query: str, provenance_results: list[Derivation]) -> str:
        """
        Generate a human-readable explanation from provenance results.
        """
        schema = await self._queries.get_schema_definition(schema_name)
        return await self._explainer.explain(sql_query, str(provenance_results), schema)
