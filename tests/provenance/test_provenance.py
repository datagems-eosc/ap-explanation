from pathlib import Path
from typing import List, TypedDict

import pytest
from sqlglot import parse_one

from provenance_demo.internal.sql_rewriter import SqlRewriter
from provenance_demo.types.semiring import DbSemiring


class QueryProvCase(TypedDict):
    query: str
    expected: str
    # Why bother testing this case
    reason: str


def _remove_sql_comments(sql: str) -> str:
    """Remove SQL comments from the given SQL string."""
    import re
    # Remove single-line comments (-- comment)
    sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
    # Remove multi-line comments (/* comment */)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql.strip()


def _load_test_cases() -> List[QueryProvCase]:
    """Load test cases from the cases directory."""
    cases_dir = Path(__file__).parent / "cases"
    test_cases = []

    for case_dir in sorted(cases_dir.iterdir()):
        if not case_dir.is_dir():
            continue

        query_file = case_dir / "query.sql"
        expected_file = case_dir / "expected.sql"

        if query_file.exists() and expected_file.exists():
            query = _remove_sql_comments(query_file.read_text())
            expected = _remove_sql_comments(expected_file.read_text())
            test_cases.append({
                "reason": case_dir.name,
                "query": query,
                "expected": expected,
            })

    return test_cases


test_cases: List[QueryProvCase] = _load_test_cases()


@pytest.mark.parametrize("case", test_cases, ids=[case["reason"] for case in test_cases])
def test_rewrite_sql(case: QueryProvCase, sql_rewriter: SqlRewriter, why_semiring: DbSemiring):
    """
    Compares the rewritten SQL with the expected one by parsing both and comparing their ASTs.
    This avoids issues with formatting differences. This will however still fail if the column order is different.
    """
    rewritten = sql_rewriter.rewrite(case["query"], why_semiring)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(case["expected"])
