from typing import LiteralString, cast

from psycopg import AsyncConnection
from psycopg.sql import SQL
from sqlglot import exp, parse_one
from sqlglot.expressions import (
    AggFunc,
    Anonymous,
    Column,
    Having,
    Literal,
    Select,
    Subquery,
)


class ProvenanceService:

    _conn: AsyncConnection

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def compute_provenance(self, sql_query: str) -> str | None:
        edited_query = self.rewrite_sql(sql_query)

        await self._conn.execute("SET search_path TO mathe, public, provsql;")

        # Note: psycopg's SQL class requires LiteralString for raw SQL strings
        # This is to prevent user SQL injection, which is not a concern here since
        # the queries are internal. We cast to satisfy the type checker.
        cursor = await self._conn.execute(SQL(cast(LiteralString, edited_query)))
        row = await cursor.fetchone()

        if row is None:
            return None

        return row[1]

    def rewrite_sql(self, query: str) -> str:
        """
        Rewrite a SQL query to return the provenance explanation
        The rewritting rules are as follows:
        - Only SELECT queries are supported
        - HAVING operators are not supported yet
        - Non-aggregate query: add whyPROV_now(provenance(), 'why_mapping')
        - Aggregate query: wrap the original query as a subquery, and add aggregation_formula(inner_aggregate_alias, 'formula_mapping') in outer SELECT

        /!\ FOR THIS TO WORK, THE whyPROV_now function, why_mapping and formula_mapping MUST BE DEFINED IN THE DATABASE /!\

        Args:
            query (str): Original SQL query
        Returns:
            str: Rewritten SQL query
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
        if any(outer_select.find_all(AggFunc)):
            return self._rewrite_sql_aggregate(query)
        else:
            return self._rewrite_sql_non_aggregate(query)

    def _rewrite_sql_non_aggregate(self, query: str) -> str:
        """
        Rewrite a non-aggregate query by adding whyPROV_now to the select list.
        Args:
            query (str): Original SQL query
        Returns:
            str: Rewritten SQL query
        """
        ast = parse_one(query)

        if not isinstance(ast, Select):
            raise ValueError("Expected SELECT query")

        ast.expressions.append(
            Anonymous(
                this="whyPROV_now",
                expressions=[
                    Anonymous(this="provenance"),
                    Literal.string("why_mapping"),
                ],
            )
        )

        return ast.sql()

    def _rewrite_sql_aggregate(self, query: str) -> str:
        """
        Rewrite an aggregate query by wrapping it as a subquery and adding
        aggregation_formula on the outer select.

        Args:
            query (str): Original SQL query
        Returns:
            str: Rewritten SQL query
        """
        ast = parse_one(query)

        outer_select = ast.find(Select)
        if outer_select is None:
            raise ValueError("Expected outer SELECT")

        # Find the first aggregate in the outer select
        agg_alias = None
        for proj in outer_select.expressions:
            if isinstance(proj.this, AggFunc):
                agg_alias = proj.alias
                break

        if agg_alias is None:
            raise ValueError("No aggregate found in query")

        # Wrap the original query as a subquery
        subquery_alias = "x"
        subquery = Subquery(this=outer_select.copy(), alias=subquery_alias)

        outer_columns = [
            col for col in outer_select.expressions if not isinstance(col.this, AggFunc)]
        outer_columns.append(
            Anonymous(
                this="aggregation_formula",
                expressions=[Column(this=agg_alias),
                             Literal.string("formula_mapping")]
            )
        )

        new_outer_select = exp.Select(
            expressions=outer_columns, from_=subquery)

        return new_outer_select.sql()
