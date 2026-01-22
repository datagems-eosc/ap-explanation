from logging import getLogger

from orjson import dumps

from provenance_demo.repository.provenance import ProvenanceRepository

logger = getLogger(__name__)


class ProvenanceService:
    """
    Service layer for provenance operations.

    Orchestrates business logic by delegating to the provenance repository.
    """

    _provenance_repo: ProvenanceRepository

    def __init__(self, provenance_repo: ProvenanceRepository):
        self._provenance_repo = provenance_repo

    async def annotate_dataset(self, table_name: str, schema_name: str) -> bool:
        """
        Annotate a table with provenance information.

        Args:
            table_name: Name of the table to annotate
            schema_name: Schema where the table is located

        Returns:
            bool: True if the table was annotated
        """
        await self._provenance_repo.enable_provenance_for(schema_name, table_name)
        return True

    async def compute_provenance(self, schema_name: str, sql_query: str) -> str | None:
        """
        Execute a SQL query with provenance tracking and return annotated results.

        Args:
            sql_query: The SQL query to execute with provenance
            schema_name: Schema where the query should be executed

        Returns:
            JSON string of results with provenance annotations
        """
        rows = await self._provenance_repo.query(schema_name, sql_query, self._provenance_repo._semiring)
        return dumps(rows).decode('utf-8')
