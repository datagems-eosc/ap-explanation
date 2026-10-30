from psycopg import AsyncConnection


class SchemaRepository:
    """
    Describes a database schema, for the LLM explaining a query on it.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_definition(self, schema_name: str) -> str:
        """
        Retrieve schema information such as table and column names for the specified schema.

        Args:
            schema_name: The name of the database schema to retrieve information for.
        Returns:
            A string containing schema information, including tables, columns, and relationships.
        """
        # NOTE: The simplest way to do this would be to use pg_dump with postgres.
        # However, we use this kind of convoluted method to be able to support a large panel of databases.
        # This may not be sufficient to support all db, in which case we can consider implementing multiple strategies for schema retrieval

        # Pseudo string builder for schema info, can be replaced with structured dict if needed
        sb = []
        async with self._conn.cursor() as cur:
            # 1. Get all tables in the schema
            await cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type='BASE TABLE'
                ORDER BY table_name
            """,
                (schema_name,),
            )
            tables = [row[0] for row in await cur.fetchall()]

            for table_name in tables:
                # 2. Get columns for this table
                await cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s
                    AND table_name = %s
                    ORDER BY ordinal_position
                """,
                    (schema_name, table_name),
                )
                columns = await cur.fetchall()

                # 3. Collect column definitions
                col_defs = []
                for col_name, data_type, is_nullable, col_default in columns:
                    nullable = "" if is_nullable == "NO" else " NULL"
                    default = f" DEFAULT {col_default}" if col_default else ""
                    col_defs.append(f"    {col_name} {data_type}{nullable}{default}")

                # 4. Print CREATE TABLE once per table
                sb.append(f"--\n-- Table: {schema_name}.{table_name}\n--")
                sb.append(
                    f"CREATE TABLE {schema_name}.{table_name} (\n"
                    + ",\n".join(col_defs)
                    + "\n);\n"
                )

                # 5. Add primary key once per table
                await cur.execute(
                    """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = %s
                    AND tc.table_name = %s
                    AND tc.constraint_type = 'PRIMARY KEY'
                    ORDER BY kcu.ordinal_position
                """,
                    (schema_name, table_name),
                )
                pk_columns = [row[0] for row in await cur.fetchall()]
                if pk_columns:
                    pk_cols = ", ".join(pk_columns)
                    sb.append(
                        f"ALTER TABLE {schema_name}.{table_name} ADD PRIMARY KEY ({pk_cols});\n"
                    )

        return "\n".join(sb)
