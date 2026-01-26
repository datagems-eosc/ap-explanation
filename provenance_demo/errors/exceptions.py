"""Custom exception classes for the provenance demo application."""


class ProvSqlMissingError(Exception):
    """
    Exception raised when the ProvSQL extension is not installed or not available
    on the PostgreSQL server.

    This error indicates that provenance operations cannot be performed because
    the required ProvSQL extension is missing from the database.
    """

    def __init__(self, message: str = "ProvSQL extension is not installed on the PostgreSQL server"):
        self.message = message
        super().__init__(self.message)


class TableOrSchemaNotFoundError(Exception):
    """
    Exception raised when a specified table or schema does not exist in the database.

    This error indicates that provenance operations cannot be performed because
    the target table or schema was not found.
    """

    def __init__(self, table_name: str | None = None, schema_name: str | None = None):
        if table_name and schema_name:
            self.message = f"Table '{table_name}' does not exist in schema '{schema_name}'"
        elif table_name:
            self.message = f"Table '{table_name}' does not exist"
        elif schema_name:
            self.message = f"Schema '{schema_name}' does not exist"
        else:
            self.message = "Table or schema does not exist"
        super().__init__(self.message)
