import csv
from pathlib import Path
from typing import List

import pytest

from ap_explanation.services.provenance import ProvenanceService
from ap_explanation.types.semiring import DbSemiring
from tests.conftest import TestSchema


@pytest.mark.asyncio
async def test_process_mathe_questions_with_provenance(
    provenance_service: ProvenanceService,
    all_semirings: List[DbSemiring],
    test_schema: TestSchema
):
    """
    Parse the CSV file with MathE questions, execute queries with provenance tracking,
    and write results to a new CSV file with:
    - nl_question: The natural language question
    - query: The original SQL query (if any)
    - rewritten_query: The SQL query rewritten with provenance tracking
    - provenance_output: The provenance data as JSON
    """

    # Define paths
    fixtures_dir = Path(__file__).parent.parent / "fixtures"
    input_csv = fixtures_dir / "mathe_questions.csv"
    output_csv = fixtures_dir / "mathe_questions_with_provenance.csv"

    # Annotate all tables in the schema with all semirings
    # We need to annotate the main table and any referenced tables
    tables_to_annotate = ["assessment", "students", "platform_materials",
                          "material_type", "platform_material_keyword",
                          "platform__keywords", "material_top_sub",
                          "platform__topic", "platform__subtopic"]

    for table in tables_to_annotate:
        try:
            await provenance_service.annotate_dataset(table, test_schema.schema, all_semirings)
        except Exception as e:
            # Some tables might not exist, that's ok
            print(f"Could not annotate {table}: {e}")

    # Read the input CSV
    results = []
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')

        for row in reader:
            nl_question = row['question'].strip().strip('"')
            query = row['query'].strip().strip('"')

            # Skip if no query or if it's N/A or a conceptual question
            if not query:
                results.append({
                    'nl_question': nl_question,
                    'query': '',
                    'rewritten_query': '',
                    'provenance_output': ''
                })
                continue

            # Skip queries with parameters (we'd need to replace them)
            if ':student' in query or ':topic' in query:
                results.append({
                    'nl_question': nl_question,
                    'query': query,
                    'rewritten_query': '',
                    'provenance_output': ''
                })
                continue

            # Try to execute the query with provenance
            rewritten_query = ''
            try:
                # Get the rewritten query for the first semiring (formula)
                from ap_explanation.internal.sql_rewriter import SqlRewriter
                rewriter = SqlRewriter()
                formula_semiring = next(
                    s for s in all_semirings if s.name == "formula")
                rewritten_query = rewriter.rewrite(query, formula_semiring)

                # Compute provenance for all semirings
                result_json = await provenance_service.compute_provenance(
                    test_schema.schema,
                    query,
                    [formula_semiring]
                )

                results.append({
                    'nl_question': nl_question,
                    'query': query,
                    'rewritten_query': rewritten_query,
                    'provenance_output': result_json
                })

            except Exception as e:
                # If query fails, record the error but keep the rewritten query if generated
                results.append({
                    'nl_question': nl_question,
                    'query': query,
                    'rewritten_query': rewritten_query,
                    'provenance_output': f'ERROR: {str(e)}'
                })
                print({
                    'nl_question': nl_question,
                    'query': query,
                    'rewritten_query': rewritten_query,
                    'provenance_output': f'ERROR: {str(e)}'
                })

    # Write results to output CSV
    with open(output_csv, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['nl_question', 'query',
                      'rewritten_query', 'provenance_output']
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='|')

        writer.writeheader()
        for result in results:
            # Escape quotes in the fields
            escaped_result = {
                key: value.replace('"', '""') if isinstance(
                    value, str) else value
                for key, value in result.items()
            }
            writer.writerow(escaped_result)

    print(f"\nProcessed {len(results)} questions")
    print(f"Results written to: {output_csv}")

    # Verify the output file was created
    assert output_csv.exists()
    assert len(results) > 0
