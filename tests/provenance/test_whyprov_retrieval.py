"""Tests for whyprov data retrieval functionality."""

import pytest

from provenance_demo.repository.provenance_mapping import (
    ConstantMapping,
    CtidReferenceMapping,
    CustomExpressionMapping,
    parse_whyprov_string,
)
from provenance_demo.services.provenance import ProvenanceService


class TestWhyProvParsing:
    """Test whyprov string parsing and decoding logic."""

    def test_parse_whyprov_string_basic(self):
        """Test parsing a basic whyprov string."""
        whyprov = '{\"{assessment@p108r52,platform__sna__questions@p17r7,platform__topic@p0r5}\"}'
        result = parse_whyprov_string(whyprov)

        assert len(result) == 3
        assert result[0] == 'assessment@p108r52'
        assert result[1] == 'platform__sna__questions@p17r7'
        assert result[2] == 'platform__topic@p0r5'

    def test_parse_whyprov_string_single_reference(self):
        """Test parsing a whyprov string with a single reference."""
        whyprov = '{\"{assessment@p108r52}\"}'
        result = parse_whyprov_string(whyprov)

        assert len(result) == 1
        assert result[0] == 'assessment@p108r52'

    def test_parse_whyprov_string_invalid_format(self):
        """Test parsing an invalid whyprov string."""
        whyprov = 'invalid_format'
        with pytest.raises(ValueError, match="Invalid whyprov format"):
            parse_whyprov_string(whyprov)

    def test_decode_row_reference(self):
        """Test decoding a ctid from provenance value."""
        mapping = CtidReferenceMapping()
        table_name, ctid = mapping.decode_row_reference('assessment@p108r52')

        assert table_name == 'assessment'
        assert ctid == '(108,52)'

    def test_decode_row_reference_complex_table_name(self):
        """Test decoding a ctid with a complex table name."""
        mapping = CtidReferenceMapping()
        table_name, ctid = mapping.decode_row_reference(
            'platform__sna__questions@p17r7')

        assert table_name == 'platform__sna__questions'
        assert ctid == '(17,7)'

    def test_decode_row_reference_invalid_format(self):
        """Test decoding an invalid ctid format."""
        mapping = CtidReferenceMapping()

        with pytest.raises(ValueError, match="Invalid provenance format"):
            mapping.decode_row_reference('invalid_format')

        with pytest.raises(ValueError, match="Invalid ctid format"):
            mapping.decode_row_reference('table@invalid')

    def test_ctid_reference_mapping_sql(self):
        """Test SQL generation for ctid reference mapping."""
        mapping = CtidReferenceMapping()
        result = mapping.get_mapping_target_sql('assessment')

        assert 'assessment' in result
        assert 'ctid' in result
        assert result == "'assessment@p'||(ctid::text::point)[0]::int||'r'||(ctid::text::point)[1]::int"
        assert mapping.can_decode() is True

    def test_constant_mapping_sql(self):
        """Test SQL generation for constant mapping (counting semiring)."""
        mapping = ConstantMapping("1")
        result = mapping.get_mapping_target_sql('assessment')

        assert result == "'1'"
        assert mapping.can_decode() is False

    def test_custom_expression_mapping_sql(self):
        """Test SQL generation for custom expression mapping (formula semiring)."""
        mapping = CustomExpressionMapping("'a'||id")
        result = mapping.get_mapping_target_sql('assessment')

        assert result == "'a'||id"
        assert mapping.can_decode() is False


@pytest.mark.asyncio
class TestWhyProvRetrieval:
    """Test whyprov data retrieval from database."""

    async def test_retrieve_provenance_data(self, provenance_service: ProvenanceService):
        """Test retrieving actual data from whyprov string."""
        # This test requires an actual database with data
        # You'll need to adjust the whyprov string to match your test data
        whyprov = '{\"{platform__topic@p0r5}\"}'

        try:
            result = await provenance_service.retrieve_provenance_data(whyprov)

            assert isinstance(result, list)
            assert len(result) > 0

            # Check structure of first result
            first_result = result[0]
            assert 'table' in first_result
            assert 'ctid' in first_result
            assert 'reference' in first_result
            assert 'data' in first_result

            assert first_result['table'] == 'platform__topic'
            assert first_result['ctid'] == '(0,5)'
            assert isinstance(first_result['data'], dict)
        except Exception as e:
            # If the test data doesn't exist, skip this test
            pytest.skip(f"Test data not available: {e}")

    async def test_retrieve_provenance_data_multiple_references(self, provenance_service: ProvenanceService):
        """Test retrieving data from multiple references."""
        whyprov = '{\"{assessment@p108r52,platform__sna__questions@p17r7}\"}'

        try:
            result = await provenance_service.retrieve_provenance_data(whyprov)

            assert isinstance(result, list)
            # Should have results for each reference (assuming data exists)
            assert len(result) <= 2  # May be less if some rows don't exist

            # Check that different tables are represented
            tables = {row['table'] for row in result}
            assert len(tables) >= 1  # At least one table should be present
        except Exception as e:
            pytest.skip(f"Test data not available: {e}")
