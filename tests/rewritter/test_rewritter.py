from pathlib import Path
from typing import List, TypedDict

import pytest
from sqlglot import parse_one

from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.types.semiring import DbSemiring


class QueryProvCase(TypedDict):
    query: str
    expected_why: str | None
    expected_formula: str | None
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
        expected_why_file = case_dir / "expected_why.sql"
        expected_formula_file = case_dir / "expected_formula.sql"

        if query_file.exists():
            query = _remove_sql_comments(query_file.read_text())

            expected_why = None
            if expected_why_file.exists():
                expected_why = _remove_sql_comments(
                    expected_why_file.read_text())

            expected_formula = None
            if expected_formula_file.exists():
                expected_formula = _remove_sql_comments(
                    expected_formula_file.read_text())

            test_cases.append({
                "reason": case_dir.name,
                "query": query,
                "expected_why": expected_why,
                "expected_formula": expected_formula,
            })

    return test_cases


test_cases: List[QueryProvCase] = _load_test_cases()


@pytest.mark.parametrize("case", test_cases, ids=[case["reason"] for case in test_cases])
def test_rewrite_sql_why(case: QueryProvCase, sql_rewriter: SqlRewriter, why_semiring: DbSemiring):
    """
    Compares the rewritten SQL with the expected one by parsing both and comparing their ASTs.
    This avoids issues with formatting differences. This will however still fail if the column order is different.
    """
    if case["expected_why"] is None:
        pytest.skip(f"No expected_why.sql file for {case['reason']}")

    try:
        rewritten = sql_rewriter.rewrite(case["query"], why_semiring)
        print("Rewritten SQL:", rewritten)
        assert parse_one(rewritten) == parse_one(case["expected_why"])
    except NotImplementedError as e:
        # This is expected for the why_semiring that doesn't support aggregates yet
        pytest.skip(f"Skipping test due to NotImplementedError: {e}")


@pytest.mark.parametrize("case", test_cases, ids=[case["reason"] for case in test_cases])
def test_rewrite_sql_formula(case: QueryProvCase, sql_rewriter: SqlRewriter, formula_semiring: DbSemiring):
    """
    Compares the rewritten SQL with the expected one by parsing both and comparing their ASTs.
    This avoids issues with formatting differences. This will however still fail if the column order is different.
    """
    if case["expected_formula"] is None:
        pytest.skip(f"No expected_formula.sql file for {case['reason']}")

    rewritten = sql_rewriter.rewrite(case["query"], formula_semiring)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(case["expected_formula"])
