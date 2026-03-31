from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, List

from psycopg import AsyncConnection

from ap_explanation.types.pg_json import PgJsonNode

from .data_source import DataSource


class RelationalDbDataSource(DataSource):
    """Represents a relational database as a data source."""

    table_nodes: List[PgJsonNode] = []

    @property
    def db_name(self) -> str:
        if not self.base_node.properties or "name" not in self.base_node.properties:
            raise ValueError(
                "Relational_Database node is missing 'name' property")
        return self.base_node.properties["name"]

    @property
    def schema_name(self) -> str:
        if not self.table_nodes:
            raise ValueError(
                "No Table nodes found in this relational database data source")
        schemas: set = set()
        for node in self.table_nodes:
            if not node.properties or "name" not in node.properties:
                raise ValueError(
                    "Some Table nodes are missing the 'name' property")
            parts = node.properties["name"].split(".", 1)
            if len(parts) != 2:
                raise ValueError(
                    f"Table name '{node.properties['name']}' is not fully qualified. "
                    "Expected format: 'schema.table'"
                )
            schemas.add(parts[0])
        if len(schemas) > 1:
            raise ValueError(
                f"All tables must belong to the same schema. Found: {', '.join(sorted(schemas))}"
            )
        return schemas.pop()

    @property
    def table_names(self) -> List[str]:
        names = []
        for node in self.table_nodes:
            if not node.properties or "name" not in node.properties:
                raise ValueError(
                    "Some Table nodes are missing the 'name' property")
            parts = node.properties["name"].split(".", 1)
            if len(parts) != 2:
                raise ValueError(
                    f"Table name '{node.properties['name']}' is not fully qualified. "
                    "Expected format: 'schema.table'"
                )
            names.append(parts[1])
        return names

    @asynccontextmanager
    async def seed_database(self, conn: AsyncConnection, src_dir: Path) -> AsyncGenerator[None, None]:
        """Relational databases don't require seeding, so this is a no-op."""
        yield
