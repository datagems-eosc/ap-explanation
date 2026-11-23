"""
Integration tests for result probabilities: tuple probabilities are set from a
table column with ``set_probabilities``, then evaluated per result row by
``compute_provenance(..., compute_probability=True)``.
"""

import pytest
import pytest_asyncio
from psycopg import AsyncConnection

from ap_explanation.domains.provenance.semiring import DbSemiring
from ap_explanation.errors import InvalidProbabilityColumnError
from ap_explanation.services.provenance import ProvenanceService
from tests.conftest import TestSchema


@pytest_asyncio.fixture(autouse=True)
async def probability_columns(db_connection: AsyncConnection, test_schema: TestSchema):
    """
    Add probability columns to the test table. Added before annotation, with
    constant defaults, so no UPDATE runs against a provenance-tracked table.
    """
    await db_connection.execute(
        f"ALTER TABLE {test_schema.schema}.{test_schema.table}"
        " ADD COLUMN reliability float8 DEFAULT 0.5,"
        " ADD COLUMN reliability_alt float8 DEFAULT 0.2,"
        " ADD COLUMN out_of_range float8 DEFAULT 1.5"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("merged_by", ["DISTINCT", "UNION"])
async def test_ok_probability_of_merged_rows(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
    two_rows_of_one_student: str,
    merged_by: str,
):
    """A row present if either of two independent 0.5 tuples is: 1 - 0.5 * 0.5."""
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)
    await provenance_service.set_probabilities(test_schema.schema, {test_schema.table: "reliability"})

    first, second = two_rows_of_one_student.split(", ")
    query = {
        "DISTINCT": f"SELECT DISTINCT student_id FROM {test_schema.table} WHERE id IN ({first}, {second})",
        "UNION": (
            f"SELECT student_id FROM {test_schema.table} WHERE id = {first}"
            f" UNION SELECT student_id FROM {test_schema.table} WHERE id = {second}"
        ),
    }[merged_by]
    derivations = await provenance_service.compute_provenance(
        test_schema.schema, query, [why_semiring], compute_probability=True)

    assert len(derivations) == 1
    assert derivations[0].probability == pytest.approx(0.75)


@pytest.mark.asyncio
async def test_ok_probability_overwritten_by_later_calls(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
    two_rows_of_one_student: str,
):
    """
    Probabilities outlive the request that set them: a later call must replace
    them, whether with another column or with certainty when none is declared.
    ProvSQL writes a probability once, so a changed one is a new input gate,
    and a new reference: the rows they resolve to must stay the same.
    """
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)
    query = f"SELECT id FROM {test_schema.table} WHERE id IN ({two_rows_of_one_student})"
    explanations: list[list[str]] = []

    async def probabilities(column: str | None) -> list[float | None]:
        await provenance_service.set_probabilities(test_schema.schema, {test_schema.table: column})
        derivations = await provenance_service.compute_provenance(
            test_schema.schema, query, [why_semiring], compute_probability=True)
        explanations.append(sorted(
            str([entry["data"] for entry in d.provenance[why_semiring.name].data]) for d in derivations
        ))
        return [d.probability for d in derivations]

    assert await probabilities("reliability") == [pytest.approx(0.5)] * 2
    assert await probabilities("reliability_alt") == [pytest.approx(0.2)] * 2
    assert await probabilities(None) == [pytest.approx(1.0)] * 2
    assert explanations[0] == explanations[1] == explanations[2]


@pytest.mark.asyncio
@pytest.mark.parametrize("column", ["out_of_range", "no_such_column"])
async def test_ko_invalid_probability_column(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
    column: str,
):
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)

    with pytest.raises(InvalidProbabilityColumnError) as exc_info:
        await provenance_service.set_probabilities(test_schema.schema, {test_schema.table: column})

    assert column in str(exc_info.value)


@pytest.mark.asyncio
async def test_ok_no_probability_unless_requested(
    provenance_service: ProvenanceService,
    why_semiring: DbSemiring,
    test_schema: TestSchema,
    two_rows_of_one_student: str,
):
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)
    await provenance_service.set_probabilities(test_schema.schema, {test_schema.table: "reliability"})

    query = f"SELECT id FROM {test_schema.table} WHERE id IN ({two_rows_of_one_student})"
    derivations = await provenance_service.compute_provenance(test_schema.schema, query, [why_semiring])

    assert len(derivations) == 2
    assert all(d.probability is None for d in derivations)


@pytest.mark.asyncio
async def test_ok_probability_of_every_group(
    provenance_service: ProvenanceService,
    formula_semiring: DbSemiring,
    test_schema: TestSchema,
):
    """
    Every group of an aggregate gets its probability: a group of k independent
    0.5 tuples exists with probability 1 - 0.5^k. Computed in a separate run,
    it was keyed by a token ProvSQL does not keep across runs for groups, and
    was lost. In formula only: the other semirings do not support aggregates.
    """
    await provenance_service.annotate_dataset(test_schema.table, test_schema.schema)
    await provenance_service.set_probabilities(test_schema.schema, {test_schema.table: "reliability"})

    query = f"SELECT student_id, COUNT(*) AS cnt FROM {test_schema.table} WHERE student_id < 50 GROUP BY student_id"
    derivations = await provenance_service.compute_provenance(
        test_schema.schema, query, [formula_semiring], compute_probability=True)

    assert len(derivations) > 1
    for d in derivations:
        # An aggregate's value reads '<value> (*)': ProvSQL's aggregation token
        count = int(d.answer["cnt"].split()[0])
        assert d.probability == pytest.approx(1 - 0.5 ** count)
