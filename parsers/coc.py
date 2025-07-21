# parsers/coc.py - Certificate of Compliance parser
from __future__ import annotations
import json
import csv
from io import StringIO
from pathlib import Path
from typing import Dict, List, Union
from .base import SSTDocumentParser, RobustParser
from .preprocessor import SSTDataPreprocessor
from .csv_parser import CertCSVParser

class CertParser(SSTDocumentParser):
    """Certificate of Compliance parser for CSV/JSON data."""
    
    def __init__(self, doc_type: str):
        self.doc_type = doc_type
        self.pre = SSTDataPreprocessor()
        self.csv_parser = CertCSVParser()
        super().__init__(self.pre)
        
    def parse(self, data: Union[str, Path], version: str) -> List[Dict]:
        """Parse CSV/JSON data for Certificate items."""
        # If data is a Path object or string path to CSV file
        if isinstance(data, Path) or (isinstance(data, str) and Path(data).exists()):
            file_path = Path(data) if isinstance(data, Path) else Path(data)
            if file_path.suffix.lower() == '.csv':
                csv_result = self.csv_parser.parse(file_path)
                # Extract items from the structured return format
                return csv_result.get('cert_items', [])
        
        # Otherwise try to parse as JSON string
        if isinstance(data, str):
            try:
                json_data = json.loads(data)
                return self._parse_json(json_data, version)
            except json.JSONDecodeError:
                # Fall back to CSV parsing if it's CSV data
                return self._parse_csv(data, version)
        
        raise ValueError("Certificate data must be a CSV file path or valid JSON")
    
    def _parse_json(self, data: Dict, version: str) -> List[Dict]:
        """Parse JSON format certificate data."""
        items = []
        
        # Handle different JSON structures based on version
        if 'cert_items' in data:
            raw_items = data['cert_items']
        elif 'compliance_items' in data:
            raw_items = data['compliance_items']
        elif 'items' in data:
            raw_items = data['items']
        else:
            # If the data is a list at the root level
            raw_items = data if isinstance(data, list) else []
        
        for item in raw_items:
            normalized = self._normalize_cert_item(item, version)
            if normalized:
                items.append(normalized)
                
        return items
    
    def _parse_csv(self, data: str, version: str) -> List[Dict]:
        """Parse CSV format certificate data."""
        reader = csv.DictReader(StringIO(data))
        
        # Use RobustParser for error recovery
        parser = RobustParser(
            lambda row: self._normalize_cert_item(row, version),
            error_threshold=0.10
        )
        
        rows = list(reader)
        return parser.parse_with_recovery(rows)
    
    def _normalize_cert_item(self, item: Dict, version: str) -> Dict:
        """Normalize a certificate item to standard format."""
        # Map various field names to standard names
        question_num = (
            item.get('question_number') or 
            item.get('question_num') or 
            item.get('number') or 
            item.get('q_num') or
            item.get('id', '')
        )
        
        question_text = (
            item.get('question_text') or 
            item.get('question') or 
            item.get('description') or
            item.get('text', '')
        )
        
        answer = str(item.get('answer', '')).strip()
        
        # Handle various answer formats
        if isinstance(item.get('answer'), bool):
            answer = 'Yes' if item.get('answer') else 'No'
        elif answer.lower() in ['true', 'false']:
            answer = 'Yes' if answer.lower() == 'true' else 'No'
        elif answer.upper() in ['Y', 'N']:
            answer = 'Yes' if answer.upper() == 'Y' else 'No'
        elif answer.upper() in ['X', '']:
            # Some formats use 'X' for yes and blank for no
            answer = 'Yes' if answer.upper() == 'X' else 'No'
            
        # Determine compliance_met based on answer
        compliance_met = None
        if answer.upper() in ['YES', 'Y']:
            compliance_met = True
        elif answer.upper() in ['NO', 'N']:
            compliance_met = False
        elif answer.upper() in ['N/A', 'NA']:
            compliance_met = None
            
        return {
            'question_number': str(question_num).strip(),
            'question_text': question_text.strip(),
            'answer': answer,
            'citation': str(item.get('citation', '')).strip(),
            'notes': str(item.get('notes', item.get('comment', ''))).strip(),
            'section': str(item.get('section', 'General')).strip(),
            'topic': str(item.get('topic', '')).strip(),
            'compliance_met': compliance_met,
            'effective_dates': str(item.get('effective_dates', '')).strip()
        }


# Alias for backward compatibility
CoCParser = CertParser