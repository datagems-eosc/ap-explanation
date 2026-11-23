"""The description of a schema handed to the LLM."""

import pytest
from psycopg import AsyncConnection

from ap_explanation.repositories.schema import SchemaRepository
from tests.conftest import TestSchema


@pytest.mark.asyncio
async def test_schema_definition_has_columns_and_primary_keys(
    db_connection: AsyncConnection, test_schema: TestSchema
):
    definition = await SchemaRepository(db_connection).get_definition(test_schema.schema)

    table = f"{test_schema.schema}.{test_schema.table}"
    assert f"CREATE TABLE {table} (\n    id " in definition
    assert "    student_id " in definition
    assert f"ALTER TABLE {table} ADD PRIMARY KEY (id);" in definition
