
from typing import Protocol, runtime_checkable


@runtime_checkable
class Explainer(Protocol):

    async def explain(self, query: str, provenance: str, database_schema: str) -> str | None:
        """
        Generate a human-readable explanation from provenance results.
        Args:
            query: The SQL query for which to generate explanation.
            provenance: The provenance information for the query.
            database_schema: The schema definition of the database.
        Returns:
                A human-readable explanation of the provenance information, or None
                if the LLM's reply has no text.
        """
        ...
