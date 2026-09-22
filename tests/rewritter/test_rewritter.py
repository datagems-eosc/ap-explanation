from pathlib import Path

import pytest
from sqlglot import parse_one

from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.types.semiring import DbSemiring

# Each case directory holds a query.sql and an expected_<name>.sql per rewriting
# it tests, <name> being a semiring name or "probability"
EXPECTATIONS = ["why", "formula", "boolexpr", "how", "which", "probability"]


def _remove_sql_comments(sql: str) -> str:
    """Remove SQL comments from the given SQL string."""
    import re
    # Remove single-line comments (-- comment)
    sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
    # Remove multi-line comments (/* comment */)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql.strip()


def _read_sql(path: Path) -> str | None:
    return _remove_sql_comments(path.read_text()) if path.exists() else None


def _load_test_cases() -> list[dict]:
    """Load test cases from the cases directory, the directory name being why the case matters."""
    cases_dir = Path(__file__).parent / "cases"
    return [
        {
            "reason": case_dir.name,
            "query": _read_sql(case_dir / "query.sql"),
            **{name: _read_sql(case_dir / f"expected_{name}.sql") for name in EXPECTATIONS},
        }
        for case_dir in sorted(cases_dir.iterdir())
        if (case_dir / "query.sql").exists()
    ]


test_cases = _load_test_cases()


@pytest.mark.parametrize("case", test_cases, ids=[case["reason"] for case in test_cases])
@pytest.mark.parametrize("expectation", EXPECTATIONS)
def test_rewrite_sql(case: dict, expectation: str, sql_rewriter: SqlRewriter, all_semirings: list[DbSemiring]):
    """
    Compares the rewritten SQL with the expected one by parsing both and comparing their ASTs.
    This avoids issues with formatting differences. This will however still fail if the column order is different.
    """
    if case[expectation] is None:
        pytest.skip(f"No expected_{expectation}.sql file for {case['reason']}")

    if expectation == "probability":
        rewritten = sql_rewriter.rewrite_probability(case["query"])
    else:
        semiring = next(s for s in all_semirings if s.name == expectation)
        rewritten = sql_rewriter.rewrite(case["query"], semiring)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(case[expectation])


def test_rewrite_sql_distinct_names_every_projection(sql_rewriter: SqlRewriter, why_semiring: DbSemiring):
    """Unnamed expressions get an alias, and stars are selected as x.*, so the wrapper can select them."""
    rewritten = sql_rewriter.rewrite(
        "SELECT DISTINCT upper(name), * FROM t", why_semiring)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(
        "SELECT x.col_0, x.*, sr_why(provenance(), 'why_mapping') "
        "FROM (SELECT DISTINCT upper(name) AS col_0, * FROM t) AS x"
    )


def test_rewrite_sql_probability_wraps_set_operations_whole(sql_rewriter: SqlRewriter):
    rewritten = sql_rewriter.rewrite_probability(
        "SELECT a FROM t1 UNION SELECT a FROM t2")
    assert parse_one(rewritten) == parse_one(
        "SELECT probability_evaluate(provenance()) FROM (SELECT a FROM t1 UNION SELECT a FROM t2) AS x"
    )


def test_rewrite_sql_probability_rejects_having(sql_rewriter: SqlRewriter):
    with pytest.raises(NotImplementedError):
        sql_rewriter.rewrite_probability(
            "SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > 1")


@pytest.mark.parametrize("case", test_cases, ids=[case["reason"] for case in test_cases])
def test_merge_provenance_and_probability(
    case: dict, sql_rewriter: SqlRewriter, why_semiring: DbSemiring
):
    """
    Proba and provenance must be able to be merged by provsql id
    """
    if case["reason"] == "aggregate":
        pytest.skip("aggregates legitimately differ: value gate vs group token")

    semiring = parse_one(sql_rewriter.rewrite(case["query"], why_semiring))
    probability = parse_one(sql_rewriter.rewrite_probability(case["query"]))

    # Drop the appended call from each, leaving the shape ProvSQL sees
    for ast in (semiring, probability):
        ast.set("expressions", ast.expressions[:-1])

    assert semiring == probability
