from pathlib import Path

import pytest
from sqlglot import parse_one

from ap_explanation.errors import AggregateSemiringError
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
        rewritten = sql_rewriter.rewrite(case["query"], [], probability=True)
    else:
        semiring = next(s for s in all_semirings if s.name == expectation)
        rewritten = sql_rewriter.rewrite(case["query"], [semiring])
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(case[expectation])


def test_rewrite_sql_distinct_names_every_projection(sql_rewriter: SqlRewriter, why_semiring: DbSemiring):
    """Unnamed expressions get an alias, and stars are selected as x.*, so the wrapper can select them."""
    rewritten = sql_rewriter.rewrite(
        "SELECT DISTINCT upper(name), EXTRACT(MONTH FROM d), * FROM t", [why_semiring])
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(
        "SELECT x.col_0, x.col_1, x.*, sr_why(provenance(), 'why_mapping') "
        "FROM (SELECT DISTINCT upper(name) AS col_0, EXTRACT(MONTH FROM d) AS col_1, * FROM t) AS x"
        " WHERE present(provenance())"
    )


def test_rewrite_sql_aggregate_names_every_group_key(sql_rewriter: SqlRewriter, formula_semiring: DbSemiring):
    """
    Unnamed group keys get an alias, so the wrapper can select them. Selected
    as x.MONTH, EXTRACT failed, and upper(t.a) became a bare x, the whole row.
    """
    rewritten = sql_rewriter.rewrite(
        "SELECT EXTRACT(MONTH FROM t.d), upper(t.a), CAST(t.b AS INT), count(*) AS n "
        "FROM t GROUP BY EXTRACT(MONTH FROM t.d), upper(t.a), CAST(t.b AS INT)",
        [formula_semiring],
    )
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(
        "SELECT x.col_0, x.col_1, x.b, x.n, sr_formula(x.n, 'formula_mapping') "
        "FROM (SELECT EXTRACT(MONTH FROM t.d) AS col_0, upper(t.a) AS col_1, CAST(t.b AS INT), count(*) AS n "
        "FROM t GROUP BY EXTRACT(MONTH FROM t.d), upper(t.a), CAST(t.b AS INT)) AS x"
        " WHERE present(provenance())"
    )


@pytest.mark.parametrize("operation", ["UNION", "UNION ALL"])
def test_rewrite_sql_wraps_set_operations_whole(
    operation: str, sql_rewriter: SqlRewriter, why_semiring: DbSemiring
):
    """A set operation is evaluated on each of its result rows, not per branch."""
    rewritten = sql_rewriter.rewrite(
        f"SELECT a FROM t1 {operation} SELECT a FROM t2", [why_semiring], probability=True)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(
        "SELECT x.*, sr_why(provenance(), 'why_mapping'), probability_evaluate(provenance())"
        f" FROM (SELECT a FROM t1 {operation} SELECT a FROM t2) AS x"
        " WHERE present(provenance())"
    )


@pytest.mark.parametrize("query", [
    "SELECT a FROM t1 EXCEPT SELECT a FROM t2",
    "SELECT a FROM t1 INTERSECT SELECT a FROM t2",
    "WITH c AS (SELECT a FROM t1 EXCEPT SELECT a FROM t2) SELECT a FROM c",
])
def test_rewrite_sql_rejects_except_and_intersect(query: str, sql_rewriter: SqlRewriter, why_semiring: DbSemiring):
    """ProvSQL refuses INTERSECT, and returns the rows EXCEPT removes."""
    with pytest.raises(NotImplementedError):
        sql_rewriter.rewrite(query, [why_semiring])


@pytest.mark.parametrize("query", [
    "INSERT INTO t SELECT a FROM t2",
    "DELETE FROM t WHERE id IN (SELECT id FROM t2)",
    "UPDATE t SET a = 1",
])
def test_rewrite_sql_rejects_non_select(query: str, sql_rewriter: SqlRewriter, why_semiring: DbSemiring):
    """A statement containing a SELECT is not a SELECT, nor wrapped like a UNION."""
    with pytest.raises(ValueError):
        sql_rewriter.rewrite(query, [why_semiring])


def test_rewrite_sql_every_semiring_and_probability_in_one_query(
    sql_rewriter: SqlRewriter, all_semirings: list[DbSemiring]
):
    """One query for all: ProvSQL's row tokens are not stable across separate runs."""
    rewritten = sql_rewriter.rewrite("SELECT a.id FROM assessment a", all_semirings, probability=True)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(
        "SELECT x.*, sr_formula(provenance(), 'formula_mapping'), sr_why(provenance(), 'why_mapping'),"
        " sr_boolexpr(provenance(), 'boolexpr_mapping'), sr_how(provenance(), 'how_mapping'),"
        " sr_which(provenance(), 'which_mapping'), probability_evaluate(provenance())"
        " FROM (SELECT a.id FROM assessment a) AS x WHERE present(provenance())"
    )


def test_rewrite_sql_aggregate_keeps_semirings_supporting_it(
    sql_rewriter: SqlRewriter, all_semirings: list[DbSemiring]
):
    """ProvSQL evaluates an aggregate in the formula semiring only, the others fail on it."""
    rewritten = sql_rewriter.rewrite("SELECT a, count(*) AS n FROM t GROUP BY a", all_semirings, probability=True)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(
        "SELECT x.a, x.n, sr_formula(x.n, 'formula_mapping'), probability_evaluate(provenance())"
        " FROM (SELECT a, count(*) AS n FROM t GROUP BY a) AS x WHERE present(provenance())"
    )


def test_rewrite_sql_aggregate_rejects_semirings_not_supporting_it(
    sql_rewriter: SqlRewriter, why_semiring: DbSemiring
):
    with pytest.raises(AggregateSemiringError):
        sql_rewriter.rewrite("SELECT a, count(*) AS n FROM t GROUP BY a", [why_semiring])


def test_rewrite_sql_aggregate_without_group_by(sql_rewriter: SqlRewriter, formula_semiring: DbSemiring):
    """Without GROUP BY too: appended to the query, the provenance of an aggregate is empty."""
    rewritten = sql_rewriter.rewrite("SELECT count(*) FROM t WHERE t.a = 1", [formula_semiring])
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(
        "SELECT x.col_0, sr_formula(x.col_0, 'formula_mapping')"
        " FROM (SELECT count(*) AS col_0 FROM t WHERE t.a = 1) AS x WHERE present(provenance())"
    )


@pytest.mark.parametrize("query", [
    "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY t.a) FROM t",
    "SELECT t.b, stddev(t.a) FROM t GROUP BY t.b",
    # Mixed in one column, ProvSQL cannot evaluate the semirings on it either
    "SELECT sum(t.a) / stddev(t.a) FROM t",
])
def test_rewrite_sql_aggregate_provsql_cannot_explain(
    query: str, sql_rewriter: SqlRewriter, formula_semiring: DbSemiring
):
    """
    ProvSQL fails evaluating a semiring on percentile_cont, stddev... Rewritten
    plainly, the provenance would be why the row exists, not its value: empty
    without GROUP BY, the group in some semirings only.
    """
    with pytest.raises(NotImplementedError, match="cannot explain"):
        sql_rewriter.rewrite(query, [formula_semiring])


def test_rewrite_sql_aggregate_explained_by_a_supported_one(
    sql_rewriter: SqlRewriter, all_semirings: list[DbSemiring]
):
    """Next to one ProvSQL cannot explain, a supported aggregate lists the rows aggregated, for both."""
    rewritten = sql_rewriter.rewrite("SELECT a, stddev(b) AS sd, count(*) AS n FROM t GROUP BY a", all_semirings)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(
        "SELECT x.a, x.sd, x.n, sr_formula(x.n, 'formula_mapping')"
        " FROM (SELECT a, stddev(b) AS sd, count(*) AS n FROM t GROUP BY a) AS x WHERE present(provenance())"
    )


def test_rewrite_sql_keeps_lateral_ordinality_column(sql_rewriter: SqlRewriter, why_semiring: DbSemiring):
    """sqlglot drops idx from LATERAL UNNEST ... AS r(v, idx), and its parser too: compare the text."""
    rewritten = sql_rewriter.rewrite(
        "SELECT r.idx FROM t, LATERAL unnest(t.a) WITH ORDINALITY AS r(v, idx)", [why_semiring])
    print("Rewritten SQL:", rewritten)
    assert "WITH ORDINALITY AS r(v, idx)" in rewritten


def test_rewrite_sql_window_function_is_not_an_aggregate(sql_rewriter: SqlRewriter, why_semiring: DbSemiring):
    rewritten = sql_rewriter.rewrite("SELECT a, sum(b) OVER (PARTITION BY a) FROM t", [why_semiring])
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(
        "SELECT x.*, sr_why(provenance(), 'why_mapping')"
        " FROM (SELECT a, sum(b) OVER (PARTITION BY a) FROM t) AS x WHERE present(provenance())"
    )


def test_rewrite_sql_limits_the_actual_result(sql_rewriter: SqlRewriter, why_semiring: DbSemiring):
    """
    ProvSQL's LIMIT k returns every row that may be among the first k in some
    world: LIMIT plain(k) truncates the actual result, and present(provenance()) keeps
    the rows of the SQL answer, in a subquery as for a union.
    """
    rewritten = sql_rewriter.rewrite(
        "SELECT a FROM t WHERE b IN (SELECT b FROM u LIMIT 2) ORDER BY a LIMIT 3", [why_semiring],
        probability=True)
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(
        "SELECT x.*, sr_why(provenance(), 'why_mapping'), probability_evaluate(provenance()) "
        "FROM (SELECT a FROM t WHERE b IN (SELECT b FROM u LIMIT plain(2)) ORDER BY a LIMIT plain(3)) AS x "
        "WHERE present(provenance())"
    )


def test_rewrite_sql_keeps_plain_limits(sql_rewriter: SqlRewriter, why_semiring: DbSemiring):
    rewritten = sql_rewriter.rewrite("SELECT a FROM t LIMIT plain(3)", [why_semiring])
    assert "PLAIN(PLAIN(" not in rewritten.upper()


@pytest.mark.parametrize(("query", "expected"), [
    ("SELECT a, count(*) AS n FROM t GROUP BY a HAVING count(*) > 1",
     "SELECT x.a, x.n, sr_formula(x.n, 'formula_mapping') "
     "FROM (SELECT a, count(*) AS n FROM t GROUP BY a HAVING count(*) > 1) AS x WHERE present(provenance())"),
    ("SELECT DISTINCT a FROM t",
     "SELECT x.a, sr_formula(provenance(), 'formula_mapping') FROM (SELECT DISTINCT a FROM t) AS x "
     "WHERE present(provenance())"),
    ("SELECT a FROM t1 UNION SELECT a FROM t2",
     "SELECT x.*, sr_formula(provenance(), 'formula_mapping') FROM (SELECT a FROM t1 UNION SELECT a FROM t2) AS x "
     "WHERE present(provenance())"),
])
def test_rewrite_sql_keeps_the_rows_of_the_sql_answer(
    query: str, expected: str, sql_rewriter: SqlRewriter, formula_semiring: DbSemiring
):
    """HAVING is supported, and every wrapping filters on present(provenance())."""
    rewritten = sql_rewriter.rewrite(query, [formula_semiring])
    print("Rewritten SQL:", rewritten)
    assert parse_one(rewritten) == parse_one(expected)
