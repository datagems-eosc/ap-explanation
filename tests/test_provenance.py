from typing import List, TypedDict

import pytest
from sqlglot import parse_one

from provenance_demo.services.provenance import ProvenanceService


class QueryProvCase(TypedDict):
    query: str
    expected: str
    # Why bother testing this case
    reason: str


test_cases: List[QueryProvCase] = [
    {
        "reason": "Non aggregate",
        "query": "SELECT distinct t.name FROM assessment a JOIN platform__sna__questions q ON(a.question_id=q.id) JOIN platform__topic t ON(t.id=q.topic) WHERE id_lect=78 AND answer=-1 AND question_level=4;",
        "expected": "SELECT distinct t.name, whyPROV_now(provenance(),'why_mapping') FROM assessment a JOIN platform__sna__questions q ON(a.question_id=q.id) JOIN platform__topic t ON(t.id=q.topic) WHERE id_lect=78 AND answer=-1 AND question_level=4;"
    },
    {
        "reason": "Aggregate",
        "query": "select t.name, count(distinct a.question_id) as nb_questions from assessment a join platform__topic t on (a.topic=t.id) where student_id=80 and answer=-1 and question_level>2 group by t.name;",
        "expected": "select name, aggregation_formula(nb_questions,'formula_mapping') from ( select t.name, count(distinct a.question_id) as nb_questions from assessment a join platform__topic t on (a.topic=t.id) where student_id=80 and answer=-1 and question_level>2 group by t.name) x;"
    }
]


@pytest.mark.parametrize("case", test_cases, ids=[case["reason"] for case in test_cases])
def test_rewrite_sql(case: QueryProvCase, unit_prov_svc: ProvenanceService):
    """
    Compares the rewritten SQL with the expected one by parsing both and comparing their ASTs.
    This avoids issues with formatting differences. This will however still fail if the column order is different.
    """
    rewritten = unit_prov_svc.rewrite_sql(case["query"])
    assert parse_one(rewritten) == parse_one(case["expected"])
