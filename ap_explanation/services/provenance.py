from logging import getLogger
from typing import List

from ap_explanation.internal.explainer import Explainer
from ap_explanation.repository.provenance import ProvenanceRepository
from ap_explanation.semirings import semirings as all_semirings
from ap_explanation.types.provenance import (
    Derivation,
)
from ap_explanation.types.semiring import DbSemiring

logger = getLogger(__name__)


class ProvenanceService:
    """
    Service layer for provenance operations.

    Orchestrates business logic by delegating to the provenance repository.
    """

    _provenance_repo: ProvenanceRepository
    _explainer: Explainer

    def __init__(self, provenance_repo: ProvenanceRepository, explainer: Explainer):
        self._provenance_repo = provenance_repo
        self._explainer = explainer

    async def annotate_dataset(self, table_name: str, schema_name: str, semirings: List[DbSemiring]) -> bool:
        """
        Annotate a table with provenance information.

        Args:
            table_name: Name of the table to annotate
            schema_name: Schema where the table is located
            semirings: List of semirings to enable for the table

        Returns:
            bool: True if the table was annotated or semirings were newly enabled, False otherwise
        """
        newly_annotated = await self._provenance_repo.enable_provenance(schema_name, table_name)

        semiring_newly_enabled = False
        for semiring in semirings:
            semiring_newly_enabled |= await self._provenance_repo.add_semiring(schema_name, table_name, semiring)

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

        await self._provenance_repo.remove_provenance(schema_name, table_name)

        # Note : This will be unused for the time being,
        # we don't support removing a single semiring, but we want to keep the option open to do so in the future, and it makes the implementation simpler for now to just remove all semirings when removing annotation
        any_removed = False
        for semiring in all_semirings:
            was_removed = await self._provenance_repo.remove_semiring(schema_name, table_name, semiring)
            any_removed |= was_removed

        return True

    async def compute_provenance(self, schema_name: str, sql_query: str, semirings: List[DbSemiring]) -> List[Derivation]:
        """
        Execute a SQL query with provenance tracking for each semiring, merge the
        results by ``provsql`` UUID, and return a JSON string.

        Each returned :class:`~ap_explanation.types.provenance.ProvenanceResult`
        contains the original query columns in ``answer`` and a per-semiring
        mapping in ``provenance``.

        Args:
            schema_name: Schema where the query should be executed
            sql_query: The SQL query to execute with provenance
            semirings: List of semiring configurations to compute

        Returns:
            List of :class:`~ap_explanation.types.provenance.ProvenanceResult` instances,
            one per result row, ordered as returned by the database.
        """

        # Keyed by ProvSQL UUID; dict preserves insertion order so result ordering is maintained.
        derivations: dict[str, Derivation] = {}

        for semiring in semirings:
            rows = await self._provenance_repo.query(schema_name, sql_query, semiring)
            for row in rows:
                if row.provsql not in derivations:
                    derivations[row.provsql] = Derivation(
                        answer=row.answer, provenance={})
                derivations[row.provsql].provenance[semiring.name] = row.provenance

        return list(derivations.values())

    async def explain(self, schema_name: str, sql_query: str, provenance_results: List[Derivation]) -> str:
        """
        Generate a human-readable explanation from provenance results.
        """
        schema = await self._provenance_repo.get_schema_definition(schema_name)
        return await self._explainer.explain(sql_query, str(provenance_results), schema)
