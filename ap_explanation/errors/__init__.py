"""Custom exceptions for the provenance demo application."""

from ap_explanation.errors.exceptions import (
    ProvSqlInternalError,
    ProvSqlMissingError,
    SemiringOperationNotSupportedError,
    TableNotAnnotatedError,
    TableOrSchemaNotFoundError,
)

__all__ = [
    "ProvSqlInternalError",
    "ProvSqlMissingError",
    "TableOrSchemaNotFoundError",
    "TableNotAnnotatedError",
    "SemiringOperationNotSupportedError",
]
