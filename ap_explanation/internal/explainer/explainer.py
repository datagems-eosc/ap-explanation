
from typing import List, Protocol

from ap_explanation.types.provenance import ProvenanceResult


class Explainer(Protocol):
    async def explain(self, provenance_results: List[ProvenanceResult]) -> str:
        """
        Generate a human-readable explanation from provenance results.
        Args:
            provenance_results: A list of ProvenanceResult objects containing query answers and their provenance information.
        Returns:
                str: A human-readable explanation of the provenance information.
        """
        ...
