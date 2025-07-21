# TODO: implement full CoC parser
class CoCParser:
    pass
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


# parsers/tap.py - Enhanced implementation
from __future__ import annotations
import json
import csv
from io import StringIO
from typing import Dict, List, Optional
import pdfplumber
import pandas as pd
from .base import SSTDocumentParser
from .preprocessor import SSTDataPreprocessor

class TAPParser(SSTDocumentParser):
    """Tax Administration Practices parser - handles CSV/JSON/PDF."""
    
    def __init__(self, doc_type: str):
        self.doc_type = doc_type
        self.pre = SSTDataPreprocessor()
        self.pdf_parser = TAPPDFParser()
        
    def parse(self, data: str, file_path: Optional[str] = None) -> List[Dict]:
        """Parse TAP data from various formats."""
        # If file_path ends with .pdf, use PDF parser
        if file_path and file_path.lower().endswith('.pdf'):
            return self._parse_pdf(file_path)
            
        # Try JSON first
        try:
            json_data = json.loads(data)
            return self._parse_json(json_data)
        except json.JSONDecodeError:
            # Fall back to CSV
            return self._parse_csv(data)
    
    def _parse_json(self, data: Dict) -> List[Dict]:
        """Parse JSON format TAP data."""
        items = []
        
        # Handle different JSON structures
        if 'tap_items' in data:
            raw_items = data['tap_items']
        elif 'practices' in data:
            raw_items = data['practices']
        else:
            raw_items = data.get('items', [])
            
        for item in raw_items:
            normalized = self._normalize_tap_item(item)
            if normalized:
                items.append(normalized)
                
        return items
    
    def _parse_csv(self, data: str) -> List[Dict]:
        """Parse CSV format TAP data."""
        reader = csv.DictReader(StringIO(data))
        items = []
        
        for row in reader:
            normalized = self._normalize_tap_item(row)
            if normalized:
                items.append(normalized)
                
        return items
    
    def _parse_pdf(self, file_path: str) -> List[Dict]:
        """Parse PDF format TAP data."""
        tables = self.pdf_parser.extract_tables(file_path)
        items = []
        
        for table in tables:
            # Process each table
            for _, row in table.iterrows():
                item = row.to_dict()
                normalized = self._normalize_tap_item(item)
                if normalized:
                    items.append(normalized)
                    
        return items
    
    def _normalize_tap_item(self, item: Dict) -> Dict:
        """Normalize a TAP item to standard format."""
        # Map various field names
        question_num = (
            item.get('question_number') or 
            item.get('number') or 
            item.get('q_num', '')
        )
        
        question_text = (
            item.get('question_text') or 
            item.get('question') or 
            item.get('practice', '')
        )
        
        answer = item.get('answer', item.get('response', ''))
        
        return {
            'question_number': str(question_num),
            'question_text': question_text.strip(),
            'answer': answer.strip(),
            'citation': item.get('citation', '').strip(),
            'notes': item.get('notes', item.get('comment', '')).strip(),
            'group_name': item.get('group', item.get('category', '')).strip(),
            'compliance_met': self.pre.normalize_boolean(
                item.get('compliance_met', item.get('compliant'))
            )
        }


class TAPPDFParser:
    """Enhanced PDF parser with better error handling."""
    
    SETTINGS = dict(
        vertical_strategy="lines",
        horizontal_strategy="text",
        snap_tolerance=3,
        join_tolerance=3,
        edge_min_length=5,
        min_words_vertical=3,
        min_words_horizontal=1,
        text_tolerance=3,
        intersection_tolerance=3,
    )
    
    def extract_tables(self, pdf_path: str) -> List[pd.DataFrame]:
        """Extract tables from TAP PDF with error recovery."""
        tables: List[pd.DataFrame] = []
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    try:
                        table_data = page.extract_table(self.SETTINGS)
                        if table_data and len(table_data) > 1:
                            df = self._process_table(table_data)
                            if not df.empty:
                                tables.append(df)
                    except Exception as e:
                        print(f"Warning: Failed to extract table from page {page_num + 1}: {e}")
                        continue
                        
        except Exception as e:
            raise ValueError(f"Failed to open PDF file: {e}")
            
        return tables
    
    def _process_table(self, raw_table) -> pd.DataFrame:
        """Process raw table data into DataFrame."""
        if not raw_table or len(raw_table) < 2:
            return pd.DataFrame()
            
        # Clean headers
        headers = [self._clean_text(h) for h in raw_table[0] if h]
        
        # Process rows
        rows = []
        for raw_row in raw_table[1:]:
            if any(cell and self._clean_text(cell) for cell in raw_row):
                # Pad row to match header length
                row = raw_row[:len(headers)]
                row.extend([''] * (len(headers) - len(row)))
                rows.append([self._clean_text(cell) for cell in row])
        
        if not rows:
            return pd.DataFrame()
            
        df = pd.DataFrame(rows, columns=headers)
        return df
    
    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        if not text:
            return ''
        # Replace newlines with spaces and clean up whitespace
        return ' '.join(text.split()).strip()