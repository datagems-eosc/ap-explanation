from typing import Any

from pydantic import BaseModel


class SemiringProvenance(BaseModel):
    """
    Provenance result for a single semiring.

    Attributes:
        expression: The raw semiring expression string produced by ProvSQL.
        data: Resolved provenance references (list of annotation dicts).
    """
    # Provenance expression as returned by ProvSQL, e.g. "table1@p10r1 AND table2@p5r3"
    expression: str

    # The complete row referenced in the provenance expression.
    # For example, if the expression references "table1@p10r1",
    # the corresponding entry in `data` would be the full annotated row from "table1" with ProvSQL UUID "p10r1"
    data: list[Any]


class ProvSQLRow(BaseModel):
    """
    Repository-level row returned by ``ProvenanceRepository.query()``.

    Captures the raw output of a single semiring pass before rows are merged
    by the service layer.
    """

    answer: dict[str, Any]
    provsql: str
    provenance: SemiringProvenance


class Derivation(BaseModel):
    """
    A derivation is a single row of the query result along with its provenance information.
    """
    # Original SQL result in {column_name: value} format, excluding ProvSQL-internal columns like "provsql".
    answer: dict[str, Any]

    # Maps each semiring name to its provenance information for this row.
    # Keys correspond to DbSemiring.name values from the semirings passed to compute_provenance.
    provenance: dict[str, SemiringProvenance]

    # Probability that this row is part of the result, given the tuple probabilities
    # of the queried tables. None when the probability was not requested.
    probability: float | None = None


class Provenance(BaseModel):
    """
    The complete provenance result for a query, consisting of multiple derivations (rows).
    """
    # All rows returned by the query, each with its answer and provenance information.
    derivations: list[Derivation]

    # Natural language explanation of the provenance, if generated
    explanation: str | None
