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
from typing import List

import pytest
from psycopg import AsyncConnection

from ap_explanation.errors import TableOrSchemaNotFoundError
from ap_explanation.internal.explainer.noop_explainer import NoOpExplainer
from ap_explanation.internal.sql_rewriter import SqlRewriter
from ap_explanation.repository.provenance import ProvenanceRepository
from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.provenance_analytical_pattern import (
    ProvenanceAnalyticalPattern,
)
from ap_explanation.types.semiring import DbSemiring

# CSV fixture files reference paths like "s3:/assessment.csv" which resolve
# to ``<src_dir>/assessment.csv``.  The real CSVs live in fixtures/real_mathe/.
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_SRC_DIR = FIXTURES_DIR / "csvs"


@pytest.mark.asyncio
async def test_e2e_explain_sql_query(
    explain_sql_query_file: Path,
    db_connection: AsyncConnection,
    sql_rewriter: SqlRewriter,
    all_semirings: List[DbSemiring],
):
    """Parse an AP fixture and run the full provenance pipeline end-to-end."""
    ap = ProvenanceAnalyticalPattern.model_validate(
        json.loads(explain_sql_query_file.read_text())
    )
    ds = ap.data_source
    query = ap.sql_operator.properties["query"]

    # Seed the database if needed (CSV sources create ephemeral schemas;
    # relational sources are no-ops).
    async with ds.seed_database(db_connection, CSV_SRC_DIR):
        repo = await ProvenanceRepository.create(db_connection, sql_rewriter)
        service = ProvenanceService(repo, NoOpExplainer())

        # 1. Annotate every table referenced by the data source
        try:
            for table_name in ds.table_names:
                await service.annotate_dataset(table_name, ds.schema_name, all_semirings)
        except TableOrSchemaNotFoundError:
            pytest.skip(
                f"Required table(s) not present in the test DB for {explain_sql_query_file.name}"
            )

        # 2. Compute provenance for all configured semirings
        derivations = await service.compute_provenance(
            ds.schema_name, query, all_semirings
        )

        # 3. Generate explanation (no-op explainer returns empty string)
        explanation = await service.explain(ds.schema_name, query, derivations)

        # --- Assertions ---
        assert derivations is not None
        assert len(derivations) > 0, (
            f"Expected at least one derivation for {explain_sql_query_file.name}"
        )

        for d in derivations:
            assert d.answer, "Each derivation must have an answer"
            assert d.provenance, "Each derivation must have provenance data"
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

        assert isinstance(explanation, str)
