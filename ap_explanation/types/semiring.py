from pydantic import BaseModel


class DbSemiring(BaseModel):
    """
    Configuration model for database semiring operations.

    A semiring defines how provenance information is computed and stored
    in the database. This is a pure data model without database-specific logic.
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

    @property
    def legacy_table_suffix(self) -> str:
        """
        Get the suffix of the per-semiring provenance tables that earlier
        versions created, one per semiring with identical contents.
        """
        return f"_prov{self.name}"

    def get_legacy_provenance_table_name_for(self, table_name: str) -> str:
        """
        Get the name earlier versions gave this semiring's provenance table.

        Example:
            >>> semiring = DbSemiring(name='why', retrieval_function='sr_why')
            >>> semiring.get_legacy_provenance_table_name_for('users')
            'users_provwhy'
        """
        return f"{table_name}{self.legacy_table_suffix}"

    @property
    def union_table_name(self) -> str:
        """
        Get the name of the union provenance table for this semiring.
        The union table aggregates provenance informations across all tables.

        Returns:
            Union table name based on semiring name
        """
        return f"{self.name}_mapping"
