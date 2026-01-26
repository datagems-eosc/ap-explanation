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


class TableNotAnnotatedError(Exception):
    """
    Exception raised when attempting to query provenance on a table that has not been
    annotated with the required semiring.

    This error indicates that the table exists but has not been prepared for provenance
    tracking with the requested semiring.
    """

    def __init__(self, table_name: str | None = None, schema_name: str | None = None, semiring_name: str | None = None):
        if table_name and schema_name and semiring_name:
            self.message = f"Table '{table_name}' in schema '{schema_name}' is not annotated with semiring '{semiring_name}'. Please annotate the table first."
        elif semiring_name:
            self.message = f"Table is not annotated with semiring '{semiring_name}'. Please annotate the table first."
        else:
            self.message = "Table is not annotated for provenance tracking. Please annotate the table first."
        super().__init__(self.message)


class SemiringOperationNotSupportedError(Exception):
    """
    Exception raised when attempting to perform an operation that is not supported
    by the specified semiring.

    This error indicates that the semiring does not have the required functionality
    for the requested operation (e.g., aggregation support).
    """

    def __init__(self, semiring_name: str | None = None, operation: str | None = None):
        if semiring_name and operation:
            self.message = f"The semiring '{semiring_name}' does not support {operation}. Please use a different semiring that supports this operation."
        elif semiring_name:
            self.message = f"The semiring '{semiring_name}' does not support this operation."
        else:
            self.message = "This operation is not supported by the selected semiring."
        super().__init__(self.message)
