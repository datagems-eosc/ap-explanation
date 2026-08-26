from abc import abstractmethod
from pathlib import Path
from typing import AsyncGenerator, List

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

    @abstractmethod
    @asynccontextmanager
    async def seed_database(self, conn: AsyncConnection, src_dir: Path) -> AsyncGenerator[None, None]:
        """Perform any necessary setup for the data source, such as creating tables or loading data."""
        ...
