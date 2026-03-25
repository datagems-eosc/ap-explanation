from ap_explanation.internal.explainer.explainer import Explainer


class NoOpExplainer(Explainer):
    """
    A no-op explainer that returns a fixed string regardless of input. Useful for testing.
    """

    async def explain(self, query: str, provenance: str, database_schema: str) -> str:
        return "No explanation"
