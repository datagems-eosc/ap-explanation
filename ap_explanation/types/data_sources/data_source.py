from abc import abstractmethod
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional

from fastapi.concurrency import asynccontextmanager
from psycopg import AsyncConnection
from pydantic import BaseModel

from ap_explanation.types.moma_graph import Node


class DataSource(BaseModel):
    """Base class for data sources."""
    base_node: Node

    @property
    @abstractmethod
    def db_name(self) -> str: ...

    @property
    @abstractmethod
    def schema_name(self) -> str: ...

    @property
    @abstractmethod
    def table_names(self) -> List[str]: ...

    @property
    @abstractmethod
    def probability_columns(self) -> Dict[str, Optional[str]]:
        """
        Map each table name to the column holding its tuples' probabilities, as
        declared by the data node's ``probabilityColumn`` property, or ``None``
        when the node declares none (every tuple of that table is certain).
        """
        ...

    def _probability_columns_of(self, nodes: List[Node]) -> Dict[str, Optional[str]]:
        """Pair *nodes* with ``table_names``, which is derived from them in the same order."""
        PROBABILITY_COLUMN_PROPERTY = "probabilityColumn"
        return {
            table_name: (node.properties or {}).get(
                PROBABILITY_COLUMN_PROPERTY)
            for node, table_name in zip(nodes, self.table_names)
        }

    @abstractmethod
    @asynccontextmanager
    async def seed_database(self, conn: AsyncConnection, src_dir: Path) -> AsyncGenerator[None, None]:
        """Perform any necessary setup for the data source, such as creating tables or loading data."""
        ...
