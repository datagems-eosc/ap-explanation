"""FastAPI dependencies for parsing and validating AP (Abstract Provenance) structures."""
from logging import getLogger
from typing import Annotated, List

from fastapi import Depends, HTTPException, status

from ap_explanation.types.pg_json import PgJson, PgJsonNode

logger = getLogger(__name__)


def extract_database_name(ap: PgJson) -> str:
    """
    Extract and validate database name from the Relational_Database node in the AP.

    Args:
        ap: The PgJson AP structure

    Returns:
        The database name from name property

    Raises:
        HTTPException: If the database node is missing or malformed
    """
    db_nodes = ap.get_nodes_by_label("Relational_Database")
    if not db_nodes or len(db_nodes) == 0:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="This AP has no Relational_Database node!"
        )

    db_node = db_nodes[0]
    if not db_node.properties or "name" not in db_node.properties:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Relational_Database node is missing 'name' property!"
        )

    return db_node.properties["name"]


def extract_schema_name(ap: PgJson) -> str:
    """
    Extract and validate schema name from fully qualified table names in the AP.
    All tables must belong to the same schema.

    Args:
        ap: The PgJson AP structure

    Returns:
        The schema name extracted from table names

    Raises:
        HTTPException: If tables are missing, not fully qualified, or belong to different schemas
    """
    tables_nodes = ap.get_nodes_by_label("Table")
    if not tables_nodes or len(tables_nodes) == 0:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="This AP has no Table nodes!"
        )

    schemas = set()

    for node in tables_nodes:
        if not node.properties or "name" not in node.properties:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Some Table nodes are missing the 'name' property!"
            )

        table_name = node.properties["name"]

        # Parse fully qualified table name (schema.table)
        parts = table_name.split(".", 1)
        if len(parts) != 2:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Table name '{table_name}' is not fully qualified. Expected format: 'schema.table'"
            )

        schema_name, _ = parts
        schemas.add(schema_name)

    # Validate all tables belong to the same schema
    if len(schemas) > 1:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"All tables must belong to the same schema. Found schemas: {', '.join(sorted(schemas))}"
        )

    return schemas.pop()


def extract_sql_operator(ap: PgJson) -> PgJsonNode:
    """
    Extract and validate the SQL operator node from the AP.

    Args:
        ap: The PgJson AP structure

    Returns:
        The first SQL operator node

    Raises:
        HTTPException: If no SQL operator is found or it's malformed
    """
    sql_nodes = ap.get_nodes_by_label("Provenance_SQL_Operator")

    match len(sql_nodes):
        case 0:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="This AP has no SQL Operators!"
            )
        case n if n > 1:
            logger.warning(
                "Multiple SQL_OPERATOR nodes detected, only the first one will be processed"
            )

    sql_node = sql_nodes[0]
    if not sql_node.properties or "query" not in sql_node.properties:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Malformed AP: SQL operator has no 'query' property"
        )

    assert sql_node.properties is not None
    return sql_node


def extract_table_names(ap: PgJson) -> List[str]:
    """
    Extract and validate table names from Table nodes in the AP.
    Returns only the table name part (without schema prefix).

    Args:
        ap: The PgJson AP structure

    Returns:
        List of table names (without schema prefix)

    Raises:
        HTTPException: If no tables are found or they're missing name properties
    """
    tables_nodes = ap.get_nodes_by_label("Table")
    if not tables_nodes or len(tables_nodes) == 0:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="This AP has no Table nodes!"
        )

    tables_names = []

    for node in tables_nodes:
        if not node.properties or "name" not in node.properties:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Some Table nodes are missing the 'name' property!"
            )

        table_name = node.properties["name"]

        # Parse fully qualified table name (schema.table)
        parts = table_name.split(".", 1)
        if len(parts) != 2:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Table name '{table_name}' is not fully qualified. Expected format: 'schema.table'"
            )

        # Extract only the table name (without schema)
        _, table_only = parts
        tables_names.append(table_only)

    return tables_names


# Type aliases for cleaner function signatures
DatabaseName = Annotated[str, Depends(extract_database_name)]
SchemaName = Annotated[str, Depends(extract_schema_name)]
SqlOperator = Annotated[PgJsonNode, Depends(extract_sql_operator)]
TableNames = Annotated[List[str], Depends(extract_table_names)]
