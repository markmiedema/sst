from __future__ import annotations
import pdfplumber, pandas as pd
from pathlib import Path
from typing import List, Dict, Union
from .preprocessor import SSTDataPreprocessor
from .csv_parser import TAPCSVParser

class TAPPDFParser:
    """Extracts tables from TAP PDF and returns list[pd.DataFrame]."""

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

    def __init__(self):
        self.csv_parser = TAPCSVParser()

    def parse(self, file_input: Union[str, Path]) -> List[Dict]:
        """Parse TAP data from CSV or PDF file."""
        # Convert to Path if string
        if isinstance(file_input, str):
            file_path = Path(file_input)
        else:
            file_path = file_input
            
        # Check file extension
        if file_path.suffix.lower() == '.csv':
            csv_result = self.csv_parser.parse(file_path)
            # Extract items from the structured return format
            return csv_result.get('tap_practices', [])
        elif file_path.suffix.lower() == '.pdf':
            return self._parse_pdf(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")

    def _parse_pdf(self, pdf_path: Path) -> List[Dict]:
        """Parse PDF file and return items."""
        tables = self.extract_tables(str(pdf_path))
        items = []
        
        question_number = 1
        for table in tables:
            # Process each table
            for _, row in table.iterrows():
                item = {
                    'question_number': str(question_number),
                    'question_text': str(row.get('Practice', row.get('Description', ''))),
                    'answer': str(row.get('Response', row.get('Answer', ''))),
                    'citation': str(row.get('Citation', '')),
                    'notes': str(row.get('Notes', row.get('Comments', ''))),
                    'group_name': str(row.get('Group', 'General'))
                }
                
                if item['question_text']:  # Only add if there's content
                    items.append(item)
                    question_number += 1
                    
        return items

    def extract_tables(self, pdf_path: str) -> List[pd.DataFrame]:
        tables: List[pd.DataFrame] = []
        with pdfplumber.open(pdf_path) as pdf:
            for pg in pdf.pages:
                tbl = pg.extract_table(self.SETTINGS)
                if tbl:
                    tables.append(self._process(tbl))
        return tables

    def _process(self, raw) -> pd.DataFrame:
        headers = [h.strip() for h in raw[0]]
        rows = [r for r in raw[1:] if any(c.strip() for c in r)]
        df = pd.DataFrame(rows, columns=headers)
        df = df.applymap(lambda x: ' '.join(x.split('\n')) if isinstance(x,str) else x)
        return df


# Alias for backward compatibility
TAPParser = TAPPDFParser