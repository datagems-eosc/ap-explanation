from sqlglot import parse_one
from sqlglot.expressions import (
    AggFunc,
    Anonymous,
    ArrayAgg,
    Avg,
    Column,
    Count,
    Except,
    Expression,
    Intersect,
    Limit,
    Literal,
    LogicalAnd,
    LogicalOr,
    Max,
    Min,
    Select,
    SetOperation,
    Star,
    Subquery,
    Sum,
    Union,
    Window,
    alias_,
)

from ap_explanation.errors import AggregateSemiringError
from ap_explanation.types.semiring import DbSemiring


class SqlRewriter:
    """
    Rewrites SQL queries to include provenance tracking functions.

    This class handles the transformation of SQL queries to add provenance
    annotations without interacting with the database.
    """

    # SQL flavor to use for parsing and generating SQL queries
    db_dialect = "postgres"

    # ProvSQL function evaluating the probability of a provenance token.
    # Unaliased, so it is also the name of the column it adds to the result.
    PROBABILITY_FUNCTION = "probability_evaluate"

    # The aggregates ProvSQL can explain (its src/Aggregation.cpp). It fails
    # evaluating a semiring on any other (percentile_cont, regr_slope...): an
    # aggregate query needs one column using only these to be explained.
    PROVSQL_AGGREGATES = (Count, Sum, Min, Max, Avg,
                          ArrayAgg, LogicalAnd, LogicalOr)

    # Alias of the original query when it is wrapped as a subquery
    SUBQUERY_ALIAS = "x"

    def rewrite(self, query: str, semirings: list[DbSemiring], probability: bool = False) -> str:
        """
        Rewrite a SQL query to return, in one query, each result row's value in
        every semiring of *semirings* and, if *probability*, its probability.

        The rewriting rules are as follows:
        - Only read queries (SELECT and UNION/UNION ALL of select) are supported. 
        - Write queries (INSERT, UPDATE, DELETE) are not supported
        - In read queries, EXCEPT and INTERSECT are not supported
        - The query keeps the rows of its SQL answer: ProvSQL reads a query in every possible
          world, so every LIMIT k is written LIMIT plain(k), the truncation of the actual result,
          and the query is wrapped as a subquery, the rows filtered by present(provenance()),
          as ProvSQL also returns the rows absent from the actual result that may be in other worlds
        - Aggregate queries are supported, but limited to the aggregates ProvSQL can explain (COUNT, SUM, MIN, MAX, AVG, ARRAY_AGG, BOOL_AND and BOOL_OR). If none of the selected columns uses only these aggregates, the query cannot be explained.
        - DISTINCT queries and UNIONs are wrapped as a subquery, the calls evaluated outside

        Args:
            query (str): Original SQL query
            semirings (list[DbSemiring]): Semirings to evaluate each row in
            probability (bool): Also evaluate each row's probability

        Returns:
            str: Rewritten SQL query

        Raises:
            ValueError: If the query is not a SELECT query
            NotImplementedError: If the query uses EXCEPT or INTERSECT
            AggregateSemiringError: If the query aggregates and none of the
                semirings supports aggregates
        """
        root = parse_one(query, dialect=self.db_dialect)

        # Supported operations are SELECT and UNION / UNION ALL.
        if not isinstance(root, (Select, SetOperation)):
            raise ValueError("Expected SELECT query")  # noqa: TRY004, the query is invalid, not its type

        # ProvSQL refuses INTERSECT, and EXCEPT
        if root.find(Except, Intersect):
            raise NotImplementedError(
                "EXCEPT and INTERSECT queries are not supported, rewrite INTERSECT as a JOIN with DISTINCT."
            )

        self._limit_actual_result(root)

        # Prepare the function calls to add to the SELECT
        # It can be either plain semiring retrieval or with probability_evaluate
        calls = [self._semiring_call(s) for s in semirings]
        if probability:
            calls.append(self._probability_call())

        # Any top-level aggregate (not in subqueries) makes it an aggregate query
        if not isinstance(root, Union) and any(self._top_level_aggregates(e) for e in root.expressions):
            rewritten = self._rewrite_aggregate(root, semirings, probability)
        # NOTE : We wrap distinct queries because if we just add the provenance calls, the DISTINCT
        # will also be applied to the provenance calls, which is not the expected result
        # from a user standpoint
        elif not isinstance(root, Union) and root.args.get("distinct"):
            rewritten = self._wrap_in_subquery(root, [*self._all_selected_columns_of(root), *calls])
        # NOTE : Filtering on present(provenance()) needs the row's provenance in an outer
        # query, so any other query (unions included) is wrapped as a subquery. HAVING keeps
        # the groups that fail it in other worlds, removed by this filter
        else:
            rewritten = self._wrap_in_subquery(root, [Column(this=Star(), table=self.SUBQUERY_ALIAS), *calls])

        rewritten = rewritten.where(Anonymous(this="present", expressions=[Anonymous(this="provenance")]))

        return rewritten.sql(dialect=self.db_dialect)

    def _rewrite_aggregate(self, select: Select, semirings: list[DbSemiring], probability: bool) -> Select:
        """
        Rewrite an aggregate SELECT query, with or without GROUP BY, by wrapping
        it as a subquery and adding the semiring calls on the outer select.
        ProvSQL has limited support for aggregates, so the query must have at least one 
        column using only aggregates ProvSQL can explain.

        Example:
            Original query:
            SELECT col1, SUM(col2) AS total
            FROM table
            GROUP BY col1;

            Rewritten query:
            SELECT x.col1, x.total, sr_formula(x.total, 'formula_mapping')
            FROM (
                SELECT col1, SUM(col2) AS total
                FROM table
                GROUP BY col1
            ) AS x;

        Args:
            select (Select): Parsed SELECT query, wrapped in place
            semirings (list[DbSemiring]): Semirings requested
            probability (bool): If true, also evaluate each row's probability

        Returns:
            Select: Rewritten query

        Raises:
            NotImplementedError: If no column aggregates with aggregates ProvSQL
                can explain only
            AggregateSemiringError: If semirings are requested but none supports
                aggregates
        """
        # Without it, the plain rewriting explains why each row exists, not its
        # value: nothing without GROUP BY, and the group in some semirings only
        explained = next((
            i for i, e in enumerate(select.expressions)
            if (aggregates := self._top_level_aggregates(e))
            and all(isinstance(a, self.PROVSQL_AGGREGATES) for a in aggregates)
        ), None)
        if explained is None:
            unsupported = sorted({
                a.sql_name() for e in select.expressions for a in self._top_level_aggregates(e)
                if not isinstance(a, self.PROVSQL_AGGREGATES)
            })
            raise NotImplementedError(
                f"Aggregates ProvSQL cannot explain: {', '.join(unsupported)}. Add a column with "
                "COUNT, SUM, MIN, MAX, AVG, ARRAY_AGG, BOOL_AND or BOOL_OR to explain the rows aggregated."
            )

        # NOTE : Only some semiring supports aggregates.
        aggregate_semirings = [s for s in semirings if s.supports_aggregates]
        if semirings and not aggregate_semirings:
            raise AggregateSemiringError([s.name for s in semirings])

        # Build the outer SELECT columns. Some attributes may refer to the inner SELECT
        # so we need to alias them in the inner SELECT if they are expressions with no alias.
        outer_columns = self._all_selected_columns_of(select)
        # Once all the original attributes are selected back, add the semiring retrieval functions and probability_evaluate(provenance()) on the outer SELECT.
        outer_columns += [
            self._semiring_call(s, outer_columns[explained].copy())
            for s in aggregate_semirings
        ]
        if probability:
            outer_columns.append(self._probability_call())

        return self._wrap_in_subquery(select, outer_columns)

    def _top_level_aggregates(self, node: Expression) -> list[AggFunc]:
        """
        The aggregate functions of *node*, without looking into subqueries, into
        an aggregate or into a window function (sum(...) OVER (...) does not group).
        """
        stop = (AggFunc, Subquery, Select, Window)
        return [n for n in node.walk(prune=lambda n: isinstance(n, stop)) if isinstance(n, AggFunc)]

    def _wrap_in_subquery(self, query: Expression, outer_columns: list[Expression]) -> Select:
        """
        Wrap *query* as the subquery SUBQUERY_ALIAS and select *outer_columns* from it.

        Example:
            _wrap_in_subquery(<SELECT DISTINCT a FROM t>, [x.a, sr_why(provenance(), 'why_mapping')])
            SELECT x.a, sr_why(provenance(), 'why_mapping') FROM (SELECT DISTINCT a FROM t) AS x
        """
        return Select(expressions=outer_columns).from_(
            Subquery(this=query, alias=self.SUBQUERY_ALIAS))

    @staticmethod
    def _limit_actual_result(root: Expression) -> None:
        """
        Write every LIMIT k as LIMIT plain(k), the truncation of the actual result:
        ProvSQL's ORDER BY ... LIMIT k otherwise returns every row that may be among
        the first k in some world, each with a condition involving all the others, a
        formula that grows with the rows ranked (over 1 GB for 10 of 31 readings).
        """
        for limit in root.find_all(Limit):
            count = limit.args.get("expression")
            if count is not None and not (isinstance(count, Anonymous) and count.name.lower() == "plain"):
                limit.set("expression", Anonymous(this="plain", expressions=[count]))

    def _all_selected_columns_of(self, select: Select) -> list[Column]:
        """
        References to every column of *select*, for the outer select once it is
        wrapped: x.<name>, or x.* for a star. An expression with no name, e.g.
        upper(name) or EXTRACT(MONTH FROM d), is aliased col_<i> in place so the
        outer select can reference it.
        """
        references = []
        for i, e in enumerate(select.expressions):
            if not e.is_star and not e.output_name:
                e = select.expressions[i] = alias_(e, f"col_{i}")
            references.append(Column(this=Star() if e.is_star else e.output_name, table=self.SUBQUERY_ALIAS))
        return references

    @staticmethod
    def _semiring_call(semiring: DbSemiring, token: Expression | None = None) -> Anonymous:
        """Build the semiring function call on *token*, by default the current row's provenance()."""
        args = [token if token is not None else Anonymous(this="provenance")]
        if semiring.mapping_table is not None:
            args.append(Literal.string(semiring.mapping_table))

        return Anonymous(this=semiring.retrieval_function, expressions=args)

    @classmethod
    def _probability_call(cls) -> Anonymous:
        """Build probability_evaluate(provenance()), the current row's probability."""
        return Anonymous(this=cls.PROBABILITY_FUNCTION, expressions=[Anonymous(this="provenance")])
