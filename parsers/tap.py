from __future__ import annotations
import pdfplumber, pandas as pd
from typing import List
from .preprocessor import SSTDataPreprocessor

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
