"""Custom exceptions for the provenance demo application."""

from ap_explanation.errors.exceptions import (
    DatabaseNotFoundError,
    ProvSqlInternalError,
    ProvSqlMissingError,
    SemiringOperationNotSupportedError,
    TableNotAnnotatedError,
    TableOrSchemaNotFoundError,
)

__all__ = [
    "DatabaseNotFoundError",
    "ProvSqlInternalError",
    "ProvSqlMissingError",
    "TableOrSchemaNotFoundError",
    "TableNotAnnotatedError",
    "SemiringOperationNotSupportedError",
]
