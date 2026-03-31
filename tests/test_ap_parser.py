import json
from pathlib import Path

import pytest

from ap_explanation.types.provenance_analytical_pattern import (
    ProvenanceAnalyticalPattern,
)


@pytest.fixture
def ap(explain_sql_query_file: Path) -> ProvenanceAnalyticalPattern:
    return ProvenanceAnalyticalPattern.model_validate(json.loads(explain_sql_query_file.read_text()))


def test_ap_parses_as_provenance_ap(ap: ProvenanceAnalyticalPattern):
    assert len(ap.nodes) > 0
    assert len(ap.edges) > 0
    assert ap.data_source is not None


def test_ap_has_sql_operator(ap: ProvenanceAnalyticalPattern):
    sql_node = ap.sql_operator
    assert sql_node is not None
    assert sql_node.properties is not None
    assert "query" in sql_node.properties


def test_ap_relational_db_source(ap: ProvenanceAnalyticalPattern):
    ds = ap.data_source
    if not hasattr(ds, 'table_nodes'):  # not a RelationalDbDataSource
        pytest.skip("AP data source is not a relational database")
    assert isinstance(ds.schema_name, str) and len(ds.schema_name) > 0
    assert isinstance(ds.db_name, str) and len(ds.db_name) > 0
    assert isinstance(ds.table_names, list) and len(ds.table_names) > 0
