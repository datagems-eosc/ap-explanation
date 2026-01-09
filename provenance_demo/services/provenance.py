from json import dumps, loads
from typing import LiteralString, cast
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row
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
        cursor = await self._conn.cursor(row_factory=dict_row).execute(SQL(cast(LiteralString, edited_query)))
        rows = await cursor.fetchall()

        # convert UUIDs to str for JSON serialization
        for row in rows:
            for k, v in row.items():
                if isinstance(v, UUID):
                    row[k] = str(v)

        return dumps(rows)

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
            return self._rewrite_sql_select_aggregate(query)
        else:
            return self._rewrite_sql_select_non_aggregate(query)

    def _rewrite_sql_select_non_aggregate(self, query: str) -> str:
        """
        Rewrite a non-aggregate SELECT query by adding whyPROV_now to the select list.

        Example :
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

    def _rewrite_sql_select_aggregate(self, query: str) -> str:
        """
        Rewrite an aggregate SELECT query by wrapping it as a subquery and adding
        aggregation_formula on the outer select.

        Example :
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
        Returns:
            str: Rewritten SQL query
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
        # TODO : what if there are multiple aggregates  or nested aggregates ?
        if len(proj_agg) == 0:
            raise ValueError("No aggregate found in query")
        agg = proj_agg[0]

        # Wrap the original query in a subquery
        subquery_alias = "x"
        subquery = Subquery(this=initial_select.copy(), alias=subquery_alias)

        # Copy all non-aggregate projections attributes from the inital select to the wrapper select
        outer_columns = []
        for attr in proj_non_agg:
            outer_columns.append(
                Column(this=attr.alias_or_name, table=subquery_alias)
            )

        outer_columns.append(
            Anonymous(
                this="aggregation_formula",
                expressions=[
                    Column(this=agg.alias_or_name),
                    Literal.string("formula_mapping")
                ]
            )
        )

        wrapper = Select(expressions=outer_columns).from_(subquery)

        return wrapper.sql()
