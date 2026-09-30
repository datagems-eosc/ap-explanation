"""Unit tests of ProvenanceService's policy, over a stub repository."""

import pytest

from ap_explanation.errors import AggregateSemiringError, ProvSqlInternalError
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.provenance import Derivation
from ap_explanation.types.semiring import DbSemiring

VALUE_GATES = "Error details: ProvSQL: provenance_evaluate_compiled: This semiring does not support value gates."


class StubRepository:
    """Fails, as ProvSQL does, on a query evaluating a semiring that does not support aggregates."""

    def __init__(self, error: str = VALUE_GATES, fails: bool = False):
        self.error = error
        self.fails = fails
        self.calls: list[list[str]] = []

    async def query(self, schema, query, semirings, compute_probability=False):
        self.calls.append([s.name for s in semirings])
        if self.fails or any(not s.supports_aggregates for s in semirings):
            raise ProvSqlInternalError(self.error)
        return [Derivation(answer={}, provenance={}, probability=None)]


@pytest.mark.asyncio
async def test_ok_value_gates_retried_with_aggregate_semirings(all_semirings: list[DbSemiring]):
    """An aggregate below the top level reaches the rows' provenance: formula only, rather than failing."""
    repo = StubRepository()
    await ProvenanceService(repo, None).compute_provenance("s", "q", all_semirings)

    assert repo.calls == [[s.name for s in all_semirings], ["formula"]]


@pytest.mark.asyncio
async def test_ko_value_gates_with_aggregate_semirings_only(formula_semiring: DbSemiring):
    """Nothing to leave out: the same query would fail again."""
    repo = StubRepository(fails=True)
    with pytest.raises(ProvSqlInternalError):
        await ProvenanceService(repo, None).compute_provenance("s", "q", [formula_semiring])
    assert repo.calls == [["formula"]]


@pytest.mark.asyncio
async def test_ko_value_gates_without_aggregate_semiring(why_semiring: DbSemiring):
    with pytest.raises(AggregateSemiringError):
        await ProvenanceService(StubRepository(), None).compute_provenance("s", "q", [why_semiring])


@pytest.mark.asyncio
async def test_ko_other_provsql_errors_not_retried(all_semirings: list[DbSemiring]):
    repo = StubRepository(error="ProvSQL: ORDER BY on the result of an aggregate function is not supported")
    with pytest.raises(ProvSqlInternalError):
        await ProvenanceService(repo, None).compute_provenance("s", "q", all_semirings)
    assert len(repo.calls) == 1
