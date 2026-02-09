import random

from sqlglot import parse_one
from sqlglot.expressions import (
    AggFunc,
    Alias,
    Anonymous,
    Column,
    Having,
    Literal,
    Select,
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

    def rewrite(self, query: str, semiring: DbSemiring) -> str:
        """
        Rewrite a SQL query to return the provenance explanation.

        The rewriting rules are as follows:
        - Only SELECT queries are supported
        - HAVING operators are not supported yet
        - Non-aggregate query: add whyPROV_now(provenance(), 'why_mapping')
        - Aggregate query: wrap the original query as a subquery, and add 
          aggregation_formula(inner_aggregate_alias, 'formula_mapping') in outer SELECT

        FOR THIS TO WORK, THE whyPROV_now function, why_mapping and formula_mapping 
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

        # Detect if the outer select contains top-level aggregates (not in subqueries)
        # AND has a GROUP BY clause (required for provsql aggregate provenance tracking)
        if not self._has_top_level_aggregates(outer_select) or not outer_select.args.get('group'):
            return self._rewrite_non_aggregate(query, semiring)

        if semiring.aggregate_function is None:
            from ap_explanation.errors import SemiringOperationNotSupportedError
            raise SemiringOperationNotSupportedError(
                semiring_name=semiring.name,
                operation="aggregate queries"
            )

        return self._rewrite_aggregate(query, semiring)

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
            SELECT col1, col2, whyPROV_now(provenance(), 'why_mapping')
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

        ast.expressions.append(
            Anonymous(
                this=semiring.retrieval_function,
                expressions=[
                    Anonymous(this="provenance"),
                    Literal.string(semiring.mapping_table),
                ],
            )
        )

        return ast.sql(dialect=self.db_dialect)

    def _rewrite_aggregate(self, query: str, semiring: DbSemiring) -> str:
        """
        Rewrite an aggregate SELECT query by wrapping it as a subquery and adding
        aggregation_formula on the outer select.

        Example:
            Original query:
            SELECT col1, SUM(col2) AS total
            FROM table
            GROUP BY col1;

            Rewritten query:
            SELECT col1, aggregation_formula(total, 'formula_mapping')
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

        # Find the first aggregate projection, this is the one that will be used in aggregation_formula
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

        outer_columns.append(
            Anonymous(
                this=semiring.aggregate_function,
                expressions=[
                    Column(this=agg.alias_or_name, table=subquery_alias),
                    Literal.string(semiring.mapping_table)
                ]
            )
        )

        wrapper = Select(expressions=outer_columns).from_(subquery)

        return wrapper.sql(dialect=self.db_dialect)
