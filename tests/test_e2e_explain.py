"""
End-to-end tests that load each ``explain_sql_query_*.json`` fixture,
parse it as a :class:`ProvenanceAnalyticalPattern`, and execute the full
annotate → compute_provenance → explain pipeline against the test database.

The parametrized ``explain_sql_query_file`` session fixture (defined in
conftest) drives one test run per fixture file.

CSV-backed APs seed an ephemeral schema from the local fixture files;
relational-DB-backed APs run against the tables already present in the
seeded test container.
"""

import json
from pathlib import Path

import pytest
from psycopg import AsyncConnection

from ap_explanation.domains.nl_explanation.noop_explainer import NoOpExplainer
from ap_explanation.domains.provenance.provenance_analytical_pattern import (
    ProvenanceAnalyticalPattern,
)
from ap_explanation.domains.provenance.semiring import DbSemiring
from ap_explanation.domains.provenance.sql_rewriter import SqlRewriter
from ap_explanation.errors import TableOrSchemaNotFoundError
from ap_explanation.repositories.seeding import seed_database
from ap_explanation.services.provenance import ProvenanceService

# CSV fixture files reference paths like "s3:/assessment.csv" which resolve
# to ``<src_dir>/assessment.csv``.  The real CSVs live in fixtures/real_mathe/.
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_SRC_DIR = FIXTURES_DIR / "csvs"


@pytest.mark.asyncio
async def test_e2e_explain_sql_query(
    explain_sql_query_file: Path,
    db_connection: AsyncConnection,
    sql_rewriter: SqlRewriter,
    all_semirings: list[DbSemiring],
):
    """Parse an AP fixture and run the full provenance pipeline end-to-end."""
    ap = ProvenanceAnalyticalPattern.model_validate(
        json.loads(explain_sql_query_file.read_text())
    )
    ds = ap.data_source

    # Seed the database if needed (CSV sources create ephemeral schemas;
    # relational sources are no-ops).
    async with seed_database(ds, db_connection, CSV_SRC_DIR):
        service = await ProvenanceService.create(db_connection, sql_rewriter, NoOpExplainer())

        # Annotate, set tuple probabilities from the fixture's probabilityColumn
        # properties (tables without one are certain), compute provenance for
        # all configured semirings, and explain (no-op explainer: empty string)
        try:
            provenance = await service.explain_ap(ap, compute_probability=True)
        except TableOrSchemaNotFoundError:
            pytest.skip(
                f"Required table(s) not present in the test DB for {explain_sql_query_file.name}"
            )
        derivations = provenance.derivations
        uncertain = any(ds.probability_columns.values())

        # --- Assertions ---
        assert derivations is not None
        assert len(derivations) > 0, (
            f"Expected at least one derivation for {explain_sql_query_file.name}"
        )

        for d in derivations:
            assert d.answer, "Each derivation must have an answer"
            assert d.provenance, "Each derivation must have provenance data"
            if uncertain:
                assert d.probability is not None and 0 < d.probability < 1, (
                    "Rows over uncertain tuples must have a probability below 1"
                )
            else:
                assert d.probability == pytest.approx(1.0), (
                    "Rows over certain tuples must have probability 1"
                )
            for semiring in all_semirings:
                assert semiring.name in d.provenance, (
                    f"Missing semiring '{semiring.name}' in provenance"
                )
                sp = d.provenance[semiring.name]
                assert sp.expression is not None, (
                    f"Provenance expression for '{semiring.name}' must not be None"
                )
                assert isinstance(sp.data, list), (
                    f"Provenance data for '{semiring.name}' must be a list"
                )

        assert isinstance(provenance.explanation, str)
