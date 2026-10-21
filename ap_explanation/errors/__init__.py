"""Custom exceptions for the provenance demo application."""

from ap_explanation.errors.exceptions import (
    AggregateSemiringError,
    DatabaseNotFoundError,
    InvalidProbabilityColumnError,
    ProvSqlInternalError,
    ProvSqlMissingError,
    QueryLimitExceededError,
    TableNotAnnotatedError,
    TableOrSchemaNotFoundError,
)

__all__ = [
    "AggregateSemiringError",
    "DatabaseNotFoundError",
    "InvalidProbabilityColumnError",
    "ProvSqlInternalError",
    "ProvSqlMissingError",
    "QueryLimitExceededError",
    "TableNotAnnotatedError",
    "TableOrSchemaNotFoundError",
]
