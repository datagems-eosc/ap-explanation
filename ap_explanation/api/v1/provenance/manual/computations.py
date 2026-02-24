from json import loads
from typing import List

from fastapi import Depends, HTTPException, status

from ap_explanation.api.v1.dependencies.ap_parser import (
    DatabaseName,
    SchemaName,
    SqlOperator,
    TableNames,
)
from ap_explanation.di import get_provenance_service_for_ap, get_semirings
from ap_explanation.errors import (
    ProvSqlInternalError,
    ProvSqlMissingError,
    SemiringOperationNotSupportedError,
    TableNotAnnotatedError,
)
from ap_explanation.types.semiring import DbSemiring


def _provenance_error_handler(e: Exception) -> None:
    """Re-raise domain errors as appropriate HTTP exceptions."""
    if isinstance(e, TableNotAnnotatedError):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))
    if isinstance(e, SemiringOperationNotSupportedError):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))
    if isinstance(e, ProvSqlInternalError):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail=f"Provenance computation failed: {str(e)}"
        )
    if isinstance(e, ProvSqlMissingError):
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"ProvSQL extension is not installed or not available on the PostgreSQL server: {str(e)}"
        )
    raise e


# ---------------------------------------------------------------------------
# POST /aps/provenance/manual/computations
# ---------------------------------------------------------------------------

async def compute_provenance_ap(
    db_name: DatabaseName,
    sql_node: SqlOperator,
    schema_name: SchemaName,
    tables_names: TableNames,
    semirings: List[DbSemiring] = Depends(get_semirings)
):
    """Compute provenance for the AP with all available semirings.

    Tables must already be annotated via POST /aps/explanation/manual/annotations.
    Returns the result synchronously and removes the annotation afterwards.
    """
    service_factory = get_provenance_service_for_ap(db_name)

    result = []
    async for service in service_factory():
        try:
            query = sql_node.properties["query"] if sql_node.properties else ""
            prov = await service.compute_provenance(schema_name, query, semirings)
            # NOTE: mitigates https://github.com/PierreSenellart/provsql/issues/67
            # Leaving provenance enabled blocks some queries; remove after computing.
            for table_name in tables_names:
                await service.remove_annotation(table_name, schema_name)
            result = loads(prov or "[]")
        except Exception as e:
            _provenance_error_handler(e)
        break  # Only process with first connection from pool

    return result


# ---------------------------------------------------------------------------
# POST /aps/provenance/manual/computations/{semiring_name}
# ---------------------------------------------------------------------------

async def compute_provenance_ap_with_semiring(
    semiring_name: str,
    db_name: DatabaseName,
    sql_node: SqlOperator,
    schema_name: SchemaName,
    all_semirings: List[DbSemiring] = Depends(get_semirings)
):
    """Compute provenance for the AP with a specific semiring.

    Tables must already be annotated via POST /aps/explanation/manual/annotations/{semiring_name}.
    Returns the result synchronously.
    """
    semiring = next(
        (s for s in all_semirings if s.name == semiring_name), None)
    if not semiring:
        available = ", ".join([s.name for s in all_semirings])
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"Semiring '{semiring_name}' not found. Available semirings: {available}"
        )

    service_factory = get_provenance_service_for_ap(db_name)

    result = []
    async for service in service_factory():
        try:
            query = sql_node.properties["query"] if sql_node.properties else ""
            prov = await service.compute_provenance(schema_name, query, [semiring])
            result = loads(prov or "[]")
        except Exception as e:
            _provenance_error_handler(e)
        break  # Only process with first connection from pool

    return result
