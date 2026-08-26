from pathlib import Path
from uuid import NAMESPACE_OID, uuid5

import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg.sql import SQL, Identifier

from ap_explanation.types.data_sources.csv_set_ds import CsvSetDataSource
from ap_explanation.types.moma_graph import Node

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
# All CSV files are stored under fixtures/real_mathe/ and referenced via s3:/ URIs
# The _resolve_local_path strips "s3:/" and joins to src_dir, so src_dir must be the
# parent such that src_dir / "real_mathe/assessment.csv" exists.
CSV_SRC_DIR = FIXTURES_DIR


def _nid(name: str):
    """A stable node id. MoMa node ids are UUIDs, so placeholders have to be too."""
    return uuid5(NAMESPACE_OID, name)


def _make_csv_source(csv_filenames: list[str], delimiter: str = ",") -> CsvSetDataSource:
    """Build a CsvSetDataSource backed by files in fixtures/real_mathe/."""
    base_node = Node(
        id=_nid("base"),
        labels=["CsvSet", "Data"],
        properties={"delimiter": delimiter},
    )
    csv_nodes = [
        Node(
            id=_nid(f"csv_{i}"),
            labels=["CSV", "Data"],
            properties={
                "name": name,
                "contentUrl": f"s3:/csvs/{name}",
            },
        )
        for i, name in enumerate(csv_filenames)
    ]
    return CsvSetDataSource(base_node=base_node, csv_nodes=csv_nodes)


async def _schema_exists(conn: AsyncConnection, schema_name: str) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            SQL("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s"),
            (schema_name,),
        )
        return await cur.fetchone() is not None


async def _table_exists(conn: AsyncConnection, schema_name: str, table_name: str) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            SQL(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s"
            ),
            (schema_name, table_name),
        )
        return await cur.fetchone() is not None


@pytest.mark.asyncio
async def test_seed_database_creates_schema_and_table(db_connection: AsyncConnection):
    """Schema and table are visible inside the seed_database context."""
    ds = _make_csv_source(["assessment.csv"])

    async with ds.seed_database(db_connection, CSV_SRC_DIR):
        assert await _schema_exists(db_connection, ds.schema_name), (
            f"Schema '{ds.schema_name}' should exist after setup"
        )
        assert await _table_exists(db_connection, ds.schema_name, "assessment"), (
            "Table 'assessment' should exist after setup"
        )


@pytest.mark.asyncio
async def test_seed_database_drops_schema_on_exit(db_connection: AsyncConnection):
    """Schema is dropped once the seed_database context exits."""
    ds = _make_csv_source(["assessment.csv"])
    schema_name = ds.schema_name

    async with ds.seed_database(db_connection, CSV_SRC_DIR):
        pass  # setup happened; teardown runs on exit

    assert not await _schema_exists(db_connection, schema_name), (
        f"Schema '{schema_name}' should have been dropped after teardown"
    )


@pytest.mark.asyncio
async def test_seed_database_multiple_tables(db_connection: AsyncConnection):
    """Multiple CSV files each produce their own table in the same schema."""
    filenames = ["assessment.csv", "platform__topic.csv"]
    ds = _make_csv_source(filenames)

    async with ds.seed_database(db_connection, CSV_SRC_DIR):
        for table_name in ds.table_names:
            assert await _table_exists(db_connection, ds.schema_name, table_name), (
                f"Table '{table_name}' should exist inside the context"
            )

    # All tables (and the schema) should be gone after the context exits
    assert not await _schema_exists(db_connection, ds.schema_name)


@pytest.mark.asyncio
async def test_seed_database_table_has_rows(db_connection: AsyncConnection):
    """The seeded table contains at least one row of data."""
    ds = _make_csv_source(["assessment.csv"])

    async with ds.seed_database(db_connection, CSV_SRC_DIR):
        async with db_connection.cursor() as cur:
            await cur.execute(
                SQL("SELECT COUNT(*) FROM {}.{}").format(
                    Identifier(ds.schema_name), Identifier("assessment")
                )
            )
            row = await cur.fetchone()
        assert row is not None and row[0] > 0, (
            "Seeded table should contain at least one row"
        )


@pytest.mark.asyncio
async def test_seed_database_teardown_on_exception(db_connection: AsyncConnection):
    """Schema is cleaned up even when an exception is raised inside the context."""
    ds = _make_csv_source(["assessment.csv"])
    schema_name = ds.schema_name

    with pytest.raises(RuntimeError, match="intentional"):
        async with ds.seed_database(db_connection, CSV_SRC_DIR):
            raise RuntimeError("intentional error")

    assert not await _schema_exists(db_connection, schema_name), (
        "Schema should be dropped even when an exception occurs inside the context"
    )


@pytest.mark.asyncio
async def test_seed_database_missing_content_url_raises(db_connection: AsyncConnection):
    """seed_database raises ValueError when a CSV node lacks 'contentUrl'."""
    base_node = Node(id=_nid("base"), labels=["CsvSet"], properties={})
    bad_node = Node(id=_nid("bad"), labels=["CSV"], properties={
                          "name": "assessment.csv"})
    ds = CsvSetDataSource(base_node=base_node, csv_nodes=[bad_node])

    with pytest.raises(ValueError, match="contentUrl"):
        async with ds.seed_database(db_connection, CSV_SRC_DIR):
            pass
