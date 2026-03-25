from typing import Any

from pydantic import BaseModel


class ProvenanceRow(BaseModel):
    """
    Repository-level row returned by ``ProvenanceRepository.query()``.

    Captures the raw output of a single semiring pass before rows are merged
    by the service layer.
    """

    answer: dict[str, Any]
    provsql: str
    expression: str
    data: list[Any]


class SemiringProvenance(BaseModel):
    """
    Provenance result for a single semiring.

    Attributes:
        expression: The raw semiring expression string produced by ProvSQL.
        data: Resolved provenance references (list of annotation dicts).
    """

    expression: str
    data: list[Any]


class ProvenanceResult(BaseModel):
    """
    A single row of ``compute_provenance`` output.

    Merges answer columns with per-semiring provenance information so that
    callers can access both without parsing raw JSON.

    Attributes:
        answer: Original query columns (excluding ProvSQL-internal columns).
        provenance: Mapping from semiring name to its computed provenance.
    """

    answer: dict[str, Any]
    provenance: dict[str, SemiringProvenance]
