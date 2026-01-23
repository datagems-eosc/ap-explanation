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
