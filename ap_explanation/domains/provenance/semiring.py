from pydantic import BaseModel

# Temporary view labelling the rows of a schema's annotated tables, which every
# semiring call reads: a label does not depend on the semiring
MAPPING_VIEW = "apx_mapping"


class DbSemiring(BaseModel):
    """
    Configuration model for database semiring operations.

    A semiring defines how provenance information is computed in the
    database. This is a pure data model without database-specific logic.
    """

    # Name of the semiring
    name: str

    # PSQL query function to compute the semiring value
    retrieval_function: str

    # Whether ProvSQL can evaluate an aggregate result (COUNT, AVG...) in this
    # semiring; the others fail on it with "does not support value gates"
    supports_aggregates: bool = False

    # ProvSQL function writing an aggregate's formula as rows of bounded size
    # (sr_formula_rows, ProvSQL 1.14), of which only the first are read: the
    # whole formula of an aggregate holds a term per aggregated row. None, or
    # an older ProvSQL, evaluates the aggregate with retrieval_function
    rows_function: str | None = None


semirings = [
    DbSemiring(
        name="formula",
        retrieval_function="sr_formula",
        supports_aggregates=True,
        rows_function="sr_formula_rows",
    ),
    DbSemiring(name="why", retrieval_function="sr_why"),
    DbSemiring(name="boolexpr", retrieval_function="sr_boolexpr"),
    DbSemiring(name="how", retrieval_function="sr_how"),
    DbSemiring(name="which", retrieval_function="sr_which"),
]
