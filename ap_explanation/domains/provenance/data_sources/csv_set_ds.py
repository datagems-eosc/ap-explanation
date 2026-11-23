import re
from pathlib import Path
from typing import Self
from uuid import uuid4

from moma_management.domain.generated.nodes.dataset.csvSet_schema import CsvSet
from moma_management.domain.generated.nodes.dataset.data_schema import Data
from pydantic import PrivateAttr, model_validator

from .data_source import DataSource


class CsvSetDataSource(DataSource):
    """Represents a set of CSV files as a data source."""

    base_node: CsvSet
    _schema_name: str = PrivateAttr(
        default_factory=lambda: "csv_" + uuid4().hex[:16])

    @model_validator(mode="after")
    def check_content_urls(self) -> Self:
        """Each CSV file is loaded from its contentUrl: refuse the AP rather than its task."""
        missing = [node.name for node in self.data_nodes if node.content_url is None]
        if missing:
            raise ValueError(
                f"CSV nodes {', '.join(missing)} are missing the 'contentUrl' property")
        return self

    @property
    def db_name(self) -> str:
        return "playground"

    @property
    def schema_name(self) -> str:
        return self._schema_name

    def table_name(self, node: Data) -> str:
        stem = Path(node.name).stem
        return re.sub(r'[^a-zA-Z0-9]+', '_', stem).strip('_').lower()
