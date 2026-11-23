from moma_management.domain.generated.nodes.dataset.data_schema import Data
from moma_management.domain.generated.nodes.dataset.relationalDatabase_schema import (
    RelationalDatabase,
)

from .data_source import DataSource


class RelationalDbDataSource(DataSource):
    """Represents a relational database as a data source."""

    base_node: RelationalDatabase

    @property
    def db_name(self) -> str:
        return self.base_node.name

    @property
    def schema_name(self) -> str:
        if not self.data_nodes:
            raise ValueError(
                "No Table nodes found in this relational database data source")
        schemas = {self._qualified_name(node)[0] for node in self.data_nodes}
        if len(schemas) > 1:
            raise ValueError(
                f"All tables must belong to the same schema. Found: {', '.join(sorted(schemas))}"
            )
        return schemas.pop()

    def table_name(self, node: Data) -> str:
        return self._qualified_name(node)[1]

    @staticmethod
    def _qualified_name(node: Data) -> tuple[str, str]:
        """The ``(schema, table)`` a Table node names as ``schema.table``."""
        schema, dot, table = node.name.partition(".")
        if not dot:
            raise ValueError(
                f"Table name '{node.name}' is not fully qualified. "
                "Expected format: 'schema.table'"
            )
        return schema, table
