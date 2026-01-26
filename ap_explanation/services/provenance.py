from logging import getLogger
from typing import List

from orjson import dumps

from ap_explanation.repository.provenance import ProvenanceRepository
from ap_explanation.types.semiring import DbSemiring

logger = getLogger(__name__)


class ProvenanceService:
    """
    Service layer for provenance operations.

    Orchestrates business logic by delegating to the provenance repository.
    """

    _provenance_repo: ProvenanceRepository

    def __init__(self, provenance_repo: ProvenanceRepository):
        self._provenance_repo = provenance_repo

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

    async def remove_annotation(self, table_name: str, schema_name: str, semirings: List[DbSemiring]) -> bool:
        """
        Remove provenance annotations from a table.

        Args:
            table_name: Name of the table to remove annotations from
            schema_name: Schema where the table is located
            semirings: List of semirings to remove from the table

        Returns:
            bool: True if any semiring was removed, False if none were found
        """
        any_removed = False
        for semiring in semirings:
            was_removed = await self._provenance_repo.remove_semiring(schema_name, table_name, semiring)
            any_removed |= was_removed

        return any_removed

    async def compute_provenance(self, schema_name: str, sql_query: str, semirings: List[DbSemiring]) -> str | None:
        """
        Execute a SQL query with provenance tracking and return annotated results.

        Args:
            sql_query: The SQL query to execute with provenance
            schema_name: Schema where the query should be executed

        Returns:
            JSON string of results with provenance annotations
        """
        # Execute queries sequentially since they share the same connection
        # and can't run concurrent transactions
        results = []
        for semiring in semirings:
            result = await self._provenance_repo.query(schema_name, sql_query, semiring)
            results.append(result)
        return dumps(results).decode('utf-8')
