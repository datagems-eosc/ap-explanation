import random

from sqlglot import parse_one
from sqlglot.expressions import (
    AggFunc,
    Alias,
    Anonymous,
    Column,
    Expression,
    Having,
    Literal,
    Select,
    Star,
    Subquery,
    alias_,
)

from ap_explanation.types.semiring import DbSemiring


class SqlRewriter:

    # SQL flavor to use for parsing and generating SQL queries
    db_dialect = "postgres"
    """
    Rewrites SQL queries to include provenance tracking functions.

    This class handles the transformation of SQL queries to add provenance
    annotations without interacting with the database.
    """

    # ProvSQL function evaluating the probability of a provenance token.
    # Unaliased, so it is also the name of the column it adds to the result.
    PROBABILITY_FUNCTION = "probability_evaluate"

    def rewrite(self, query: str, semiring: DbSemiring) -> str:
        """
        Rewrite a SQL query to return the provenance explanation.

        The rewriting rules are as follows:
        - Only SELECT queries are supported
        - HAVING operators are not supported yet
        - Non-aggregate query: add sr_why(provenance(), 'why_mapping')
        - DISTINCT query: wrap the original query as a subquery, and add
          sr_why(provenance(), 'why_mapping') in outer SELECT
        - Aggregate query: wrap the original query as a subquery, and add 
          sr_formula(inner_aggregate_alias, 'formula_mapping') in outer SELECT

        FOR THIS TO WORK, THE why_mapping and formula_mapping 
        MUST BE DEFINED IN THE DATABASE /!\

        Args:
            query (str): Original SQL query
            semiring (DbSemiring): Semiring configuration for provenance tracking

        Returns:
            str: Rewritten SQL query

        Raises:
            ValueError: If the query is not a SELECT query
            NotImplementedError: If the query uses HAVING or the semiring doesn't
                                support aggregate queries
        """
        outer_select = self._parse_outer_select(query)

        # Detect if the outer select contains top-level aggregates (not in subqueries)
        # AND has a GROUP BY clause (required for provsql aggregate provenance tracking)
        if not self._has_top_level_aggregates(outer_select) or not outer_select.args.get('group'):
            return self._rewrite_non_aggregate(query, semiring)

        return self._rewrite_aggregate(query, semiring)

    def rewrite_probability(self, query: str) -> str:
        """
        Rewrite a SQL query to return the probability of each result row.

        The query is always wrapped as a subquery, and only the probability is
        selected: the answer columns come from the semiring passes, merged by
        the ``provsql`` token ProvSQL appends. For a GROUP BY query, the
        probability is that of the group existing.

        Example:
            Original query:
            SELECT DISTINCT col1
            FROM table;

            Rewritten query:
            SELECT probability_evaluate(provenance())
            FROM (
                SELECT DISTINCT col1
                FROM table
            ) AS x;

        Args:
            query (str): Original SQL query

        Returns:
            str: Rewritten SQL query

        Raises:
            ValueError: If the query is not a SELECT query
            NotImplementedError: If the query uses HAVING
        """
        outer_select = self._parse_outer_select(query)

        # Evaluated outside the query, on each result row's token. Appended to
        # the query itself, provenance() would be computed per input tuple,
        # before DISTINCT merges rows, and the probability value would become
        # part of the DISTINCT key. The root is wrapped, so a set operation
        # (UNION, ...) is wrapped whole.
        wrapper = Select(
            expressions=[
                Anonymous(
                    this=self.PROBABILITY_FUNCTION,
                    expressions=[Anonymous(this="provenance")],
                )
            ]
        ).from_(Subquery(this=outer_select.root(), alias="x"))

        return wrapper.sql(dialect=self.db_dialect)

    def _parse_outer_select(self, query: str) -> Select:
        """
        Parse a query and return its outer SELECT, rejecting what the rewriting
        does not support.

        Raises:
            ValueError: If the query is not a SELECT query
            NotImplementedError: If the query uses HAVING
        """
        ast = parse_one(query, dialect=self.db_dialect)

        outer_select = ast.find(Select)
        if outer_select is None:
            raise ValueError("Expected SELECT query")

        # Note : HAVING detection is done on the outer select only
        # I honestly don't know if having can appear in subqueries without breaking the provenance logic
        # TODO: Circle back on this
        if any(outer_select.find_all(Having)):
            raise NotImplementedError(
                "HAVING queries are not supported yet, rewrite your SQL with nested SELECTs."
            )

        return outer_select

    def _has_top_level_aggregates(self, select: Select) -> bool:
        """
        Check if a SELECT statement contains aggregate functions at the top level,
        excluding aggregates that are inside subqueries.

        Args:
            select (Select): The SELECT statement to check

        Returns:
            bool: True if top-level aggregates are found, False otherwise
        """
        for expr in select.expressions:
            if self._contains_aggregate_not_in_subquery(expr):
                return True
        return False

    def _contains_aggregate_not_in_subquery(self, node) -> bool:
        """
        Recursively check if a node contains an aggregate function,
        but stop traversing when encountering a subquery.

        Args:
            node: The expression node to check

        Returns:
            bool: True if aggregate is found (not in a subquery), False otherwise
        """
        if isinstance(node, AggFunc):
            return True

        # Don't traverse into subqueries or nested SELECT statements
        if isinstance(node, (Subquery, Select)):
            return False

        # Recursively check children
        for child in node.iter_expressions():
            if self._contains_aggregate_not_in_subquery(child):
                return True

        return False

    def _rewrite_non_aggregate(self, query: str, semiring: DbSemiring) -> str:
        """
        Rewrite a non-aggregate SELECT query by adding whyPROV_now to the select list.

        Example:
            Original query:
            SELECT col1, col2
            FROM table
            WHERE condition;

            Rewritten query:
            SELECT col1, col2, sr_why(provenance(), 'why_mapping')
            FROM table
            WHERE condition;

        Args:
            query (str): Original SQL query
            semiring (DbSemiring): Semiring configuration for provenance tracking

        Returns:
            str: Rewritten SQL query

        Raises:
            ValueError: If the query is not a SELECT query
        """
        ast = parse_one(query, dialect=self.db_dialect)

        if not isinstance(ast, Select):
            raise ValueError("Expected SELECT query")

        if ast.args.get("distinct"):
            return self._rewrite_distinct(ast, semiring)

        ast.expressions.append(self._semiring_call(semiring))

        return ast.sql(dialect=self.db_dialect)

    def _rewrite_distinct(self, select: Select, semiring: DbSemiring) -> str:
        """
        Rewrite a DISTINCT SELECT query by wrapping it as a subquery and adding
        the semiring function on the outer select.

        Appended to the query itself, the semiring value would be computed per
        input tuple and become part of the DISTINCT key, so a result row merged
        from several tuples would come back once per tuple. Evaluated outside,
        it applies to each result row's token, which combines all of them.

        Example:
            Original query:
            SELECT DISTINCT t.col1
            FROM table t;

            Rewritten query:
            SELECT x.col1, sr_why(provenance(), 'why_mapping')
            FROM (
                SELECT DISTINCT t.col1
                FROM table t
            ) AS x;

        Args:
            select (Select): Parsed DISTINCT SELECT query
            semiring (DbSemiring): Semiring configuration for provenance tracking

        Returns:
            str: Rewritten SQL query
        """
        subquery_alias = "x"
        outer_columns = []

        for i, e in enumerate(select.expressions):
            if e.is_star:
                outer_columns.append(Column(this=Star(), table=subquery_alias))
                continue
            if not e.alias_or_name:
                # An unnamed expression (e.g. upper(name)) needs a name to be selected from the subquery
                e = alias_(e, f"col_{i}")
                select.expressions[i] = e
            outer_columns.append(Column(this=e.alias_or_name, table=subquery_alias))

        outer_columns.append(self._semiring_call(semiring))

        wrapper = Select(expressions=outer_columns).from_(
            Subquery(this=select, alias=subquery_alias)
        )

        return wrapper.sql(dialect=self.db_dialect)

    def _semiring_call(self, semiring: DbSemiring, token: Expression | None = None) -> Anonymous:
        """Build the semiring function call on *token*, by default the current row's provenance()."""
        args = [token if token is not None else Anonymous(this="provenance")]
        if semiring.mapping_table is not None:
            args.append(Literal.string(semiring.mapping_table))

        return Anonymous(this=semiring.retrieval_function, expressions=args)

    def _rewrite_aggregate(self, query: str, semiring: DbSemiring) -> str:
        """
        Rewrite an aggregate SELECT query by wrapping it as a subquery and adding
        sr_formula on the outer select.

        Example:
            Original query:
            SELECT col1, SUM(col2) AS total
            FROM table
            GROUP BY col1;

            Rewritten query:
            SELECT col1, sr_formula(total, 'formula_mapping')
            FROM (
                SELECT col1, SUM(col2) AS total
                FROM table
                GROUP BY col1
            ) AS x;

        Args:
            query (str): Original SQL query
            semiring (DbSemiring): Semiring configuration for provenance tracking

        Returns:
            str: Rewritten SQL query

        Raises:
            ValueError: If the query is not a SELECT query or has no aggregates
        """
        ast = parse_one(query, dialect=self.db_dialect)

        initial_select = ast.find(Select)
        if initial_select is None:
            raise ValueError("Expected query to be a SELECT query")

        # Process projections to separate aggregate and non-aggregate projections
        proj_agg = []
        proj_non_agg = []
        alias_counter = random.randint(1000, 9999)

        for i, e in enumerate(initial_select.expressions):
            if self._contains_aggregate_not_in_subquery(e):
                # Ensure aggregate expressions have an alias
                if not isinstance(e, Alias):
                    alias_name = f"agg_result_{alias_counter}"
                    alias_counter += 1
                    # Replace the expression with an aliased version
                    aliased_expr = alias_(e, alias_name)
                    initial_select.expressions[i] = aliased_expr
                    proj_agg.append(aliased_expr)
                else:
                    proj_agg.append(e)
            else:
                proj_non_agg.append(e)

        # Find the first aggregate projection, this is the one that will be used in the aggregate semiring function
        if len(proj_agg) == 0:
            raise ValueError("No aggregate found in query")
        agg = proj_agg[0]

        # Wrap the original query in a subquery (the modifications to expressions should be included)
        subquery_alias = "x"
        subquery = Subquery(this=initial_select, alias=subquery_alias)

        # Copy all non-aggregate projections attributes from the initial select to the wrapper select
        outer_columns = []
        for attr in proj_non_agg:
            outer_columns.append(
                Column(this=attr.alias_or_name, table=subquery_alias)
            )

        outer_columns.append(self._semiring_call(
            semiring, Column(this=agg.alias_or_name, table=subquery_alias)))

        wrapper = Select(expressions=outer_columns).from_(subquery)

        return wrapper.sql(dialect=self.db_dialect)
