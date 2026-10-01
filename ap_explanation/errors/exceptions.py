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
        self.table_name = table_name
        self.schema_name = schema_name
        super().__init__(table_name, schema_name)

    @property
    def message(self) -> str:
        if self.table_name and self.schema_name:
            return f"Table '{self.table_name}' does not exist in schema '{self.schema_name}'"
        elif self.table_name:
            return f"Table '{self.table_name}' does not exist"
        elif self.schema_name:
            return f"Schema '{self.schema_name}' does not exist"
        else:
            return "Table or schema does not exist"

    def __str__(self) -> str:
        return self.message


class TableNotAnnotatedError(Exception):
    """
    Exception raised when attempting to query provenance on a table that has not been
    annotated with the required semiring.

    This error indicates that the table exists but has not been prepared for provenance
    tracking with the requested semiring.
    """

    def __init__(self, table_name: str | None = None, schema_name: str | None = None, semiring_name: str | None = None):
        self.table_name = table_name
        self.schema_name = schema_name
        self.semiring_name = semiring_name
        super().__init__(table_name, schema_name, semiring_name)

    @property
    def message(self) -> str:
        if self.table_name and self.schema_name and self.semiring_name:
            return (
                f"Table '{self.table_name}' in schema '{self.schema_name}' is not annotated "
                f"with semiring '{self.semiring_name}'. Please annotate the table first."
            )
        elif self.semiring_name:
            return f"Table is not annotated with semiring '{self.semiring_name}'. Please annotate the table first."
        else:
            return "Table is not annotated for provenance tracking. Please annotate the table first."

    def __str__(self) -> str:
        return self.message


class ProvSqlInternalError(Exception):
    """
    Exception raised when ProvSQL encounters an internal error during query execution.

    This error typically occurs when provenance functions are called on tables that
    no longer have provenance annotations, or when there's a conflict between
    concurrent operations on provenance-annotated tables.
    """

    def __init__(self, message: str = "ProvSQL internal error occurred. The table may have lost its provenance annotations or there may be a conflict with concurrent operations."):
        self.message = message
        super().__init__(self.message)


class AggregateSemiringError(Exception):
    """
    Exception raised when an aggregate query is explained only with semirings
    that cannot evaluate aggregates.

    ProvSQL evaluates an aggregate result (COUNT, AVG, MAX...) with the formula
    semiring only; the others fail on it ("does not support value gates").
    """

    def __init__(self, semiring_names: list[str]):
        self.semiring_names = semiring_names
        super().__init__(semiring_names)

    @property
    def message(self) -> str:
        return (
            "Aggregate results can only be explained with the formula semiring; "
            f"requested: {', '.join(self.semiring_names)}"
        )

    def __str__(self) -> str:
        return self.message


class InvalidProbabilityColumnError(Exception):
    """
    Exception raised when a table's probability column cannot be used as tuple probabilities.

    This error indicates that the column named by a data node's ``probabilityColumn``
    property does not exist, holds non-numeric values, or holds values outside [0, 1].
    """

    def __init__(self, table_name: str, column: str, reason: str):
        self.table_name = table_name
        self.column = column
        self.reason = reason
        super().__init__(table_name, column, reason)

    @property
    def message(self) -> str:
        return f"Invalid probability column '{self.column}' on table '{self.table_name}': {self.reason}"

    def __str__(self) -> str:
        return self.message


class DatabaseNotFoundError(Exception):
    """
    Exception raised when a database cannot be found on either the Postgres or Timescale instance.

    This error indicates that the specified database does not exist on any of the
    configured database servers.
    """

    def __init__(self, db_name: str):
        self.db_name = db_name
        super().__init__(db_name)

    @property
    def message(self) -> str:
        return f"Database '{self.db_name}' not found on either Postgres or Timescale instance"

    def __str__(self) -> str:
        return self.message
