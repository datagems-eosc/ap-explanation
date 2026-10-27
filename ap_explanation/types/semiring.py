
from typing import Any

from pydantic import BaseModel, ConfigDict


class DbSemiring(BaseModel):
    """
    Configuration model for database semiring operations.

    A semiring defines how provenance information is computed and stored
    in the database. This is a pure data model without database-specific logic.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Name of the semiring
    name: str

    # PSQL query function to compute the semiring value
    retrieval_function: str

    # Name of the mapping table in the database.
    # When None, the semiring function is called without a mapping argument
    # (e.g. sr_boolexpr which has an optional mapping in newer provsql versions).
    mapping_table: str | None = None

    # Whether ProvSQL can evaluate an aggregate result (COUNT, AVG...) in this
    # semiring; the others fail on it with "does not support value gates"
    supports_aggregates: bool = False

    # ProvSQL function writing an aggregate's formula as rows of bounded size
    # (sr_formula_rows, ProvSQL 1.14), of which only the first are read: the
    # whole formula of an aggregate holds a term per aggregated row. None, or
    # an older ProvSQL, evaluates the aggregate with retrieval_function
    rows_function: str | None = None

    # Provenance mapping strategy instance
    # This determines how database rows are mapped to provenance values
    # Using Any since ProvenanceMapping is a Protocol and cannot be used with isinstance
    mappingStrategy: Any

    @property
    def table_suffix(self) -> str:
        """
        Get the suffix of the provenance tables this semiring reads.

        A provenance table only depends on the mapping strategy, so it is named
        after the strategy and shared by every semiring using it.
        """
        return f"_prov{self.mappingStrategy.name}"

    @property
    def legacy_table_suffix(self) -> str:
        """
        Get the suffix of the per-semiring provenance tables that earlier
        versions created, one per semiring with identical contents.
        """
        return f"_prov{self.name}"

    def get_provenance_table_name_for(self, table_name: str) -> str:
        """
        Get the name of the provenance table for a given base table.

        Args:
            table_name: The base table name

        Returns:
            Provenance table name with the mapping strategy's suffix

        Example:
            >>> from ap_explanation.repository.mapping.key_mapping import KeyMapping
            >>> semiring = DbSemiring(name='why', retrieval_function='sr_why', mappingStrategy=KeyMapping())
            >>> semiring.get_provenance_table_name_for('users')
            'users_provkey'
        """
        return f"{table_name}{self.table_suffix}"

    def get_legacy_provenance_table_name_for(self, table_name: str) -> str:
        """
        Get the name earlier versions gave this semiring's provenance table.

        Example:
            >>> from ap_explanation.repository.mapping.key_mapping import KeyMapping
            >>> semiring = DbSemiring(name='why', retrieval_function='sr_why', mappingStrategy=KeyMapping())
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
