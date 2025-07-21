# parsers/coc.py - Complete implementation
from __future__ import annotations
import json
import csv
from io import StringIO
from typing import Dict, List
from .base import SSTDocumentParser, RobustParser
from .preprocessor import SSTDataPreprocessor

class CertParser(SSTDocumentParser):
    """Certificate of Compliance parser."""
    
    def __init__(self, doc_type: str):
        self.doc_type = doc_type
        self.pre = SSTDataPreprocessor()
        
    def parse(self, data: str, version: str) -> List[Dict]:
        """Parse CSV/JSON data for Certificate items."""
        # Try JSON first
        try:
            json_data = json.loads(data)
            return self._parse_json(json_data, version)
        except json.JSONDecodeError:
            # Fall back to CSV
            return self._parse_csv(data, version)
    
    def _parse_json(self, data: Dict, version: str) -> List[Dict]:
        """Parse JSON format certificate data."""
        items = []
        
        # Handle different JSON structures based on version
        if 'cert_items' in data:
            raw_items = data['cert_items']
        elif 'compliance_items' in data:
            raw_items = data['compliance_items']
        else:
            raw_items = data.get('items', [])
        
        for item in raw_items:
            normalized = self._normalize_cert_item(item, version)
            if normalized:
                items.append(normalized)
                
        return items
    
    def _parse_csv(self, data: str, version: str) -> List[Dict]:
        """Parse CSV format certificate data."""
        reader = csv.DictReader(StringIO(data))
        items = []
        
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
            item.get('q_num', '')
        )
        
        question_text = (
            item.get('question_text') or 
            item.get('question') or 
            item.get('description', '')
        )
        
        answer = item.get('answer', '')
        
        # Handle various answer formats
        if isinstance(answer, bool):
            answer = 'Yes' if answer else 'No'
        elif answer.lower() in ['true', 'false']:
            answer = 'Yes' if answer.lower() == 'true' else 'No'
            
        return {
            'question_number': str(question_num),
            'question_text': question_text.strip(),
            'answer': answer.strip(),
            'citation': item.get('citation', '').strip(),
            'notes': item.get('notes', '').strip(),
            'section': item.get('section', '').strip(),
            'compliance_met': self.pre.normalize_boolean(
                item.get('compliance_met', item.get('compliant'))
            )
        }