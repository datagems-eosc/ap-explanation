from pydantic import BaseModel

# View of a schema's mapping tables, which every semiring call reads: a mapping
# does not depend on the semiring
MAPPING_VIEW = "provkey_mapping"


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
