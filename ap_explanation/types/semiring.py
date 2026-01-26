
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
    # For non aggregate queries
    retrieval_function: str

    # For aggregate queries (optional)
    aggregate_function: str | None = None

    # Name of the mapping table in the database
    mapping_table: str

    # Provenance mapping strategy instance
    # This determines how database rows are mapped to provenance values
    # Using Any since ProvenanceMapping is a Protocol and cannot be used with isinstance
    mappingStrategy: Any

    @property
    def table_suffix(self) -> str:
        """
        Get the suffix used for provenance tables in this semiring.
        """
        return f"_prov{self.name}"

    def get_provenance_table_name_for(self, table_name: str) -> str:
        """
        Get the name of the provenance table for a given base table.

        Args:
            table_name: The base table name

        Returns:
            Provenance table name with semiring suffix

        Example:
            >>> semiring = DbSemiring(name='why', ...)
            >>> semiring.get_provenance_table_name_for('users')
            'users_provwhy'
        """
        return f"{table_name}{self.table_suffix}"

    @property
    def union_table_name(self) -> str:
        """
        Get the name of the union provenance table for this semiring.
        The union table aggregates provenance informations across all tables.

        Returns:
            Union table name based on semiring name
        """
        return f"{self.name}_mapping"
