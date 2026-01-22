import pytest

from provenance_demo.internal.sql_rewriter import SqlRewriter
from provenance_demo.repository.mapping.ctid_mapping import CtidMapping
from provenance_demo.types.semiring import DbSemiring


@pytest.fixture(scope="session")
def sql_rewriter():
    """SQL rewriter for testing query transformations."""
    return SqlRewriter()


@pytest.fixture(scope="session")
def why_semiring():
    """Why provenance semiring configuration for testing."""
    return DbSemiring(
        name="why",
        retrieval_function="whyprov_now",
        aggregate_function="aggregation_formula",
        mapping_table="why_mapping",
        mappingStrategy=CtidMapping()
    )
