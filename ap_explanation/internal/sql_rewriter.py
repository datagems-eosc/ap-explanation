from sqlglot import parse_one
from sqlglot.expressions import (
    AggFunc,
    Anonymous,
    Column,
    Having,
    Literal,
    Select,
    Subquery,
)

from ap_explanation.types.semiring import DbSemiring


class SqlRewriter:
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
        ast = parse_one(query)

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

        # Detect if the outer select contains aggregates
        if not any(outer_select.find_all(AggFunc)):
            return self._rewrite_non_aggregate(query, semiring)

        if semiring.aggregate_function is None:
            from ap_explanation.errors import SemiringOperationNotSupportedError
            raise SemiringOperationNotSupportedError(
                semiring_name=semiring.name,
                operation="aggregate queries"
            )

        return self._rewrite_aggregate(query, semiring)

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
        ast = parse_one(query)

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

        return ast.sql()

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
        ast = parse_one(query)

        initial_select = ast.find(Select)
        if initial_select is None:
            raise ValueError("Expected query to be a SELECT query")

        # Process projections to separate aggregate and non-aggregate projections
        proj_agg = []
        proj_non_agg = []
        for e in initial_select.expressions:
            if e.find(AggFunc):
                proj_agg.append(e)
            else:
                proj_non_agg.append(e)

        # Find the first aggregate projection, this is the one that will be used in aggregation_formula
        # TODO : what if there are multiple aggregates or nested aggregates ?
        if len(proj_agg) == 0:
            raise ValueError("No aggregate found in query")
        agg = proj_agg[0]

        # Wrap the original query in a subquery
        subquery_alias = "x"
        subquery = Subquery(this=initial_select.copy(), alias=subquery_alias)

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
                    Column(this=agg.alias_or_name),
                    Literal.string(semiring.mapping_table)
                ]
            )
        )

        wrapper = Select(expressions=outer_columns).from_(subquery)

        return wrapper.sql()
