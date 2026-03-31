import csv
import logging
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, List
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.sql import SQL, Identifier, Literal
from pydantic import PrivateAttr

from ap_explanation.types.pg_json import PgJsonNode

from .data_source import DataSource

logger = logging.getLogger(__name__)


class CsvSetDataSource(DataSource):
    """Represents a set of CSV files as a data source."""

    csv_nodes: List[PgJsonNode] = []
    _schema_name: str = PrivateAttr(
        default_factory=lambda: "csv_" + uuid4().hex[:16])

    @property
    def db_name(self) -> str:
        return "playground"

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @property
    def table_names(self) -> List[str]:
        names = []
        for node in self.csv_nodes:
            if not node.properties or "name" not in node.properties:
                raise ValueError(
                    "Some CSV nodes are missing the 'name' property")
            stem = Path(node.properties["name"]).stem
            safe = re.sub(r'[^a-zA-Z0-9]+', '_', stem).strip('_').lower()
            names.append(safe)
        return names

    @asynccontextmanager
    async def seed_database(self, conn: AsyncConnection, src_dir: Path) -> AsyncGenerator[None, None]:
        """Create a new schema and populate it with tables based on the CSV nodes.
        The schema is dropped when the context exits.
        Args:
            conn: An active database connection
            src_dir: The local directory where CSV files are stored

        Yields:
            None, but the database will be seeded with the CSV data for the duration of the context

        Raises:
            ValueError: If any CSV node is missing required properties or if the local CSV file does
        """
        schema = self.schema_name

        for node in self.csv_nodes:
            if not node.properties or "contentUrl" not in node.properties:
                raise ValueError(
                    f"CSV node '{node.id}' is missing the 'contentUrl' property"
                )
        try:
            await self._setup(conn, Identifier(schema), src_dir)
            yield
        finally:
            await self._teardown(conn=conn, schema=Identifier(schema))

    async def _setup(self, conn: AsyncConnection, schema: Identifier, src_dir: Path) -> None:
        """Create a new schema and populate it with tables based on the CSV nodes."""

        await conn.execute(SQL("CREATE SCHEMA {}").format(schema))
        logger.info("Created playground schema '%s'", schema)

        for node, table_name in zip(self.csv_nodes, self.table_names):

            content_url = node.properties["contentUrl"]
            local_path = self._resolve_local_path(content_url, src_dir)
            assert local_path.exists(
            ), f"Local file '{local_path}' does not exist for CSV node '{node.id}'"

            await self._create_table_from_path(conn, schema, local_path, table_name)
            logger.info(
                "Seeded table '%s.%s' from '%s'", schema, table_name, content_url
            )

    async def _create_table_from_path(self, conn: AsyncConnection, schema: Identifier, local_path: Path, table_name: str) -> None:
        """Create a table in the database by reading the header and contents of the CSV file."""
        delimiter = (self.base_node.properties or {}).get("delimiter", ",")

        with local_path.open(encoding="utf-8", newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            headers = next(reader)

        # Create the table
        col_defs = (
            SQL(", ")
            .join(SQL("{} TEXT").format(Identifier(h.strip())) for h in headers)
        )
        create_table_query = (
            SQL("CREATE TABLE {}.{} ({})")
            .format(schema, Identifier(table_name), col_defs)
        )
        await conn.execute(create_table_query)

        # Fill it with data, the buffer size is limited to avoid eventual memory issues
        with local_path.open("rb") as f:
            cpy_query = (
                SQL("COPY {}.{} FROM STDIN WITH (FORMAT csv, DELIMITER {}, HEADER true)")
                .format(schema, Identifier(table_name), Literal(delimiter))
            )
            async with conn.cursor().copy(cpy_query) as copy:
                while chunk := f.read(2 ** 16):
                    await copy.write(chunk)

    async def _teardown(self, conn: AsyncConnection, schema: Identifier) -> None:
        try:
            query = SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(schema)
            await conn.execute(query)
            logger.debug("Dropped playground schema '%s'", schema)
        except Exception:
            logger.warning(
                "Failed to drop playground schema '%s' — it may need manual cleanup",
                schema,
                exc_info=True,
            )

    def _resolve_local_path(self, content_url: str, base_dir: Path) -> Path:
        """Map 's3:/some/path.csv' to '{mount}/some/path.csv'."""
        sanitized = content_url.removeprefix("s3:/").lstrip("/")
        return base_dir / sanitized
