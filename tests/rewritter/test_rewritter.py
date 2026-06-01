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
    expected_boolexpr: str | None
    expected_how: str | None
    expected_which: str | None
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
        expected_boolexpr_file = case_dir / "expected_boolexpr.sql"
        expected_how_file = case_dir / "expected_how.sql"
        expected_which_file = case_dir / "expected_which.sql"

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

            expected_boolexpr_file = case_dir / "expected_boolexpr.sql"
            expected_boolexpr = None
            if expected_boolexpr_file.exists():
                expected_boolexpr = _remove_sql_comments(
                    expected_boolexpr_file.read_text())

            expected_how_file = case_dir / "expected_how.sql"
            expected_how = None
            if expected_how_file.exists():
                expected_how = _remove_sql_comments(
                    expected_how_file.read_text())

            expected_which_file = case_dir / "expected_which.sql"
            expected_which = None
            if expected_which_file.exists():
                expected_which = _remove_sql_comments(
                    expected_which_file.read_text())

            test_cases.append({
                "reason": case_dir.name,
                "query": query,
                "expected_why": expected_why,
                "expected_formula": expected_formula,
                "expected_boolexpr": expected_boolexpr,
                "expected_how": expected_how,
                "expected_which": expected_which,
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


@pytest.mark.parametrize("case", test_cases, ids=[case["reason"] for case in test_cases])
def test_rewrite_sql_boolexpr(case: QueryProvCase, sql_rewriter: SqlRewriter, boolexpr_semiring: DbSemiring):
    """
    Compares the rewritten SQL with the expected one by parsing both and comparing their ASTs.
    """
    if case["expected_boolexpr"] is None:
        pytest.skip(f"No expected_boolexpr.sql file for {case['reason']}")

    rewritten = sql_rewriter.rewrite(case["query"], boolexpr_semiring)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(case["expected_boolexpr"])


@pytest.mark.parametrize("case", test_cases, ids=[case["reason"] for case in test_cases])
def test_rewrite_sql_how(case: QueryProvCase, sql_rewriter: SqlRewriter, how_semiring: DbSemiring):
    """
    Compares the rewritten SQL with the expected one by parsing both and comparing their ASTs.
    """
    if case["expected_how"] is None:
        pytest.skip(f"No expected_how.sql file for {case['reason']}")

    rewritten = sql_rewriter.rewrite(case["query"], how_semiring)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(case["expected_how"])


@pytest.mark.parametrize("case", test_cases, ids=[case["reason"] for case in test_cases])
def test_rewrite_sql_which(case: QueryProvCase, sql_rewriter: SqlRewriter, which_semiring: DbSemiring):
    """
    Compares the rewritten SQL with the expected one by parsing both and comparing their ASTs.
    """
    if case["expected_which"] is None:
        pytest.skip(f"No expected_which.sql file for {case['reason']}")

    rewritten = sql_rewriter.rewrite(case["query"], which_semiring)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(case["expected_which"])
