"""
Test suite for SST parsers.
Tests LOD, CERT, and TAP parsers with various input formats.
"""

import pytest
from pathlib import Path
import json

# Import parsers
from parsers.lod import LODParser
from parsers.coc import CertParser
from parsers.tap import TAPParser

# Test fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestLODParser:
    """Test Library of Definitions parser."""
    
    def setup_method(self):
        self.parser = LODParser("LOD")
    
    def test_parse_csv_format(self):
        """Test parsing LOD CSV format."""
        fixture_file = FIXTURES_DIR / "sample_lod.csv"
        data = fixture_file.read_text(encoding='utf-8')
        
        result = self.parser.parse(data, "v2024.1")
        
        assert len(result) == 4  # Should parse 4 items
        
        # Check first item
        first_item = result[0]
        assert first_item['item_type'] == 'admin_definition'
        assert first_item['code'] == 'ADM001'
        assert first_item['description'] == 'Administrative code for general use'
        assert first_item['taxable'] is True
        assert first_item['exempt'] is False
        
        # Check holiday item with threshold
        holiday_item = next(item for item in result if item['item_type'] == 'holiday_item')
        assert holiday_item['threshold'] == 100.0
    
    def test_parse_empty_data(self):
        """Test parser handles empty data gracefully."""
        result = self.parser.parse("", "v2024.1")
        assert result == []
    
    def test_parse_malformed_csv(self):
        """Test parser handles malformed CSV."""
        malformed_data = "item_type,code\nadmin,ADM001\n,missing_code"
        result = self.parser.parse(malformed_data, "v2024.1")
        
        # Should still parse valid rows
        assert len(result) >= 1


class TestCertParser:
    """Test Certificate of Compliance parser."""
    
    def setup_method(self):
        self.parser = CertParser("CERT")
    
    def test_parse_csv_format(self):
        """Test parsing CERT CSV format."""
        fixture_file = FIXTURES_DIR / "sample_cert.csv"
        data = fixture_file.read_text(encoding='utf-8')
        
        result = self.parser.parse(data, "v2024.1")
        
        assert len(result) == 3  # Should parse 3 items
        
        # Check first item
        first_item = result[0]
        assert first_item['question_number'] == '1'
        assert first_item['question_text'] == 'Does the state have a sales tax?'
        assert first_item['answer'] == 'Yes'
        assert first_item['citation'] == 'State Code 123.45'
        assert first_item['compliance_met'] is True
    
    def test_parse_json_format(self):
        """Test parsing CERT JSON format."""
        fixture_file = FIXTURES_DIR / "sample_cert.json"
        data = fixture_file.read_text(encoding='utf-8')
        
        result = self.parser.parse(data, "v2024.1")
        
        assert len(result) == 2  # Should parse 2 items from JSON
        
        # Check first item
        first_item = result[0]
        assert first_item['question_number'] == '1'
        assert first_item['answer'] == 'Yes'
        assert first_item['compliance_met'] is True
    
    def test_normalize_answers(self):
        """Test answer normalization."""
        # Test boolean to string conversion
        test_data = {
            'question_number': '1',
            'question_text': 'Test question',
            'answer': True,
            'citation': '',
            'notes': '',
            'section': '',
            'compliance_met': True
        }
        
        normalized = self.parser._normalize_cert_item(test_data, "v2024.1")
        assert normalized['answer'] == 'Yes'
        
        # Test false conversion
        test_data['answer'] = False
        normalized = self.parser._normalize_cert_item(test_data, "v2024.1")
        assert normalized['answer'] == 'No'


class TestTAPParser:
    """Test Tax Administration Practices parser."""
    
    def setup_method(self):
        self.parser = TAPParser("TAP")
    
    def test_parse_csv_format(self):
        """Test parsing TAP CSV format."""
        fixture_file = FIXTURES_DIR / "sample_tap.csv"
        data = fixture_file.read_text(encoding='utf-8')
        
        # Note: TAPParser expects different interface - adjust if needed
        result = self.parser.extract_tables(str(fixture_file))
        
        assert len(result) >= 1  # Should extract at least one table
        
        # Check that we got a DataFrame
        df = result[0]
        assert len(df) == 3  # Should have 3 rows
        assert 'question_number' in df.columns or any('question' in col.lower() for col in df.columns)


class TestParserIntegration:
    """Integration tests for parser workflow."""
    
    def test_all_parsers_handle_empty_input(self):
        """Test all parsers handle empty input gracefully."""
        parsers = [
            LODParser("LOD"),
            CertParser("CERT"), 
        ]
        
        for parser in parsers:
            result = parser.parse("", "v2024.1")
            assert isinstance(result, list)
            assert len(result) == 0
    
    def test_version_handling(self):
        """Test that parsers accept different version formats."""
        parser = CertParser("CERT")
        fixture_file = FIXTURES_DIR / "sample_cert.csv"
        data = fixture_file.read_text(encoding='utf-8')
        
        # Test different version formats
        versions = ["v2024.1", "2024.1", "v2023.0"]
        
        for version in versions:
            result = parser.parse(data, version)
            assert len(result) > 0  # Should parse successfully
    
    def test_field_mapping_consistency(self):
        """Test that parsers map fields consistently."""
        # Test CERT parser field mapping
        cert_parser = CertParser("CERT")
        
        # Test with alternative field names
        test_item = {
            'question_num': '1',  # Alternative to question_number
            'question': 'Test question',  # Alternative to question_text
            'response': 'Yes',  # Alternative to answer
            'compliance_met': True
        }
        
        normalized = cert_parser._normalize_cert_item(test_item, "v2024.1")
        
        assert normalized['question_number'] == '1'
        assert normalized['question_text'] == 'Test question'
        assert normalized['answer'] == 'Yes'
        assert normalized['compliance_met'] is True


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])