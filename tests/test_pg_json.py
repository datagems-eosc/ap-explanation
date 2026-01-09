
import pytest

from provenance_demo.types.pg_json import PgJson

SAMPLE_AP = "fixtures/ap_sql_select.json"


def test_ap_parsing():
    with open(SAMPLE_AP) as f:
        PgJson.model_validate_json(f.read())
