import re
from pathlib import Path
from uuid import uuid4

from moma_management.domain.generated.nodes.node_schema import Node
from pydantic import Field, PrivateAttr

from .data_source import DataSource


class CsvSetDataSource(DataSource):
    """Represents a set of CSV files as a data source."""

    csv_nodes: list[Node] = Field(default_factory=list)
    _schema_name: str = PrivateAttr(
        default_factory=lambda: "csv_" + uuid4().hex[:16])

    @property
    def db_name(self) -> str:
        return "playground"

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @property
    def table_names(self) -> list[str]:
        names = []
        for node in self.csv_nodes:
            if not node.properties or "name" not in node.properties:
                raise ValueError(
                    "Some CSV nodes are missing the 'name' property")
            stem = Path(node.properties["name"]).stem
            safe = re.sub(r'[^a-zA-Z0-9]+', '_', stem).strip('_').lower()
            names.append(safe)
        return names

    @property
    def probability_columns(self) -> dict[str, str | None]:
        return self._probability_columns_of(self.csv_nodes)
