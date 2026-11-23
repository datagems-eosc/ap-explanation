from pathlib import Path

import pytest
from moma_management.domain.generated.nodes.dataset.csvSet_schema import CsvSet
from moma_management.domain.generated.nodes.dataset.data_schema import Data
from psycopg import AsyncConnection
from psycopg.sql import SQL, Identifier

from ap_explanation.domains.provenance.data_sources.csv_set_ds import CsvSetDataSource
from ap_explanation.repositories.seeders import seed_database

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
# All CSV files are stored under fixtures/real_mathe/ and referenced via s3:/ URIs
# The _resolve_local_path strips "s3:/" and joins to src_dir, so src_dir must be the
# parent such that src_dir / "real_mathe/assessment.csv" exists.
CSV_SRC_DIR = FIXTURES_DIR


def _make_csv_source(csv_filenames: list[str], delimiter: str = ",") -> CsvSetDataSource:
    """Build a CsvSetDataSource backed by files in fixtures/real_mathe/."""
    return CsvSetDataSource(
        base_node=CsvSet(name="csvs", delimiter=delimiter),
        # By alias: pyright only knows the field's name, and with extra="allow"
        # content_url= would land in model_extra
        data_nodes=[Data.model_validate({"name": name, "contentUrl": f"s3:/csvs/{name}"})
                   for name in csv_filenames],
    )


async def _schema_exists(conn: AsyncConnection, schema_name: str) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            SQL("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s"),
            (schema_name,),
        )
        return await cur.fetchone() is not None


@pytest.mark.asyncio
async def test_seed_database_loads_each_csv_for_the_context(db_connection: AsyncConnection):
    """Each CSV file is a table holding its rows, in a schema dropped when the context exits."""
    ds = _make_csv_source(["assessment.csv", "platform__topic.csv"])

    async with seed_database(ds, db_connection, CSV_SRC_DIR):
        for table in ds.table_names:
            cursor = await db_connection.execute(
                SQL("SELECT COUNT(*) FROM {}.{}").format(Identifier(ds.schema_name), Identifier(table))
            )
            row = await cursor.fetchone()
            assert row is not None and row[0] > 0, f"Table '{table}' should hold the rows of its file"

    assert not await _schema_exists(db_connection, ds.schema_name)


@pytest.mark.asyncio
async def test_seed_database_teardown_on_exception(db_connection: AsyncConnection):
    """Schema is cleaned up even when an exception is raised inside the context."""
    ds = _make_csv_source(["assessment.csv"])
    schema_name = ds.schema_name

    with pytest.raises(RuntimeError, match="intentional"):
        async with seed_database(ds, db_connection, CSV_SRC_DIR):
            raise RuntimeError("intentional error")

    assert not await _schema_exists(db_connection, schema_name), (
        "Schema should be dropped even when an exception occurs inside the context"
    )


def test_csv_source_without_content_url_is_refused():
    """A CSV node without 'contentUrl' cannot be loaded: refused when the AP is parsed."""
    with pytest.raises(ValueError, match="contentUrl"):
        CsvSetDataSource(base_node=CsvSet(name="csvs"), data_nodes=[Data(name="assessment.csv")])
