import json
from pathlib import Path

import pytest

from ap_explanation.domains.provenance.provenance_analytical_pattern import (
    ProvenanceAnalyticalPattern,
)


@pytest.fixture
def ap(explain_sql_query_file: Path) -> ProvenanceAnalyticalPattern:
    return ProvenanceAnalyticalPattern.model_validate(json.loads(explain_sql_query_file.read_text()))


def test_ap_without_query_is_refused(explain_sql_query_file: Path):
    raw = json.loads(explain_sql_query_file.read_text())
    operator = next(n for n in raw["nodes"] if ProvenanceAnalyticalPattern.PROVENANCE_OP in n["labels"])
    del operator["properties"]["query"]

    with pytest.raises(ValueError, match="'query'"):
        ProvenanceAnalyticalPattern.model_validate(raw)


def test_ap_survives_a_wire_round_trip(ap: ProvenanceAnalyticalPattern):
    """
    to_wire() output must parse back into an AP.

    The managed endpoints hand this dict to Celery, which re-validates it in
    the worker — so a dump that loses the ``from`` edge alias only fails there,
    well away from the request that caused it.
    """
    dumped = ap.to_wire()

    assert all("from" in e for e in dumped["edges"]), "edges lost the 'from' alias"
    assert not any("from_" in e for e in dumped["edges"])

    reparsed = ProvenanceAnalyticalPattern.model_validate(dumped)
    assert reparsed.data_source.table_names == ap.data_source.table_names
    assert reparsed.query == ap.query


def test_ap_probability_column_read_from_data_node(explain_sql_query_file: Path):
    raw = json.loads(explain_sql_query_file.read_text())
    data_nodes = [n for n in raw["nodes"] if {"Table", "CSV"} & set(n["labels"])]
    data_nodes[0]["properties"]["probabilityColumn"] = "reliability"

    ap = ProvenanceAnalyticalPattern.model_validate(raw)
    columns = ap.data_source.probability_columns

    assert list(columns) == ap.data_source.table_names
    assert list(columns.values()).count("reliability") == 1
    assert list(columns.values()).count(None) == len(data_nodes) - 1
    # The property must reach the Celery worker too
    reparsed = ProvenanceAnalyticalPattern.model_validate(ap.to_wire())
    assert reparsed.data_source.probability_columns == columns


def test_ap_reads_contained_in_both_ways(explain_sql_query_file: Path):
    """containedIn runs from the item to its container, as moma has it; APs written
    the other way round still find the same tables."""
    raw = json.loads(explain_sql_query_file.read_text())
    reversed_raw = json.loads(explain_sql_query_file.read_text())
    for edge in reversed_raw["edges"]:
        if "containedIn" in edge["labels"]:
            edge["from"], edge["to"] = edge["to"], edge["from"]

    tables = ProvenanceAnalyticalPattern.model_validate(raw).data_source.table_names
    assert tables
    assert ProvenanceAnalyticalPattern.model_validate(reversed_raw).data_source.table_names == tables

