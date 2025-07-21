from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import Optional

class SSTDataPreprocessor:
    """Shared helpers: encoding, booleans, dates, column rename."""

    BOOL_TRUE  = {'X','x','Y','y','Yes','YES','1',True,'true','TRUE'}
    BOOL_FALSE = {'','N','n','No','NO','0',False,'false','FALSE',None}

    COLUMN_MAPPINGS = {
        'Treatment': 'taxability_status',
        'Taxable': 'taxable',
        'Exempt': 'exempt',
        'Compliance Met': 'compliance_met',
        'Comments': 'comment',
        'Notes': 'notes',
    }

    # ---------- encoding ----------
    @staticmethod
    def normalize_encoding(path: Path) -> str:
        raw = path.read_bytes()
        if b'\x00' in raw[:1000]:
            return raw.decode('utf-16le')
        try:
            return raw.decode('utf-8-sig')
        except UnicodeDecodeError:
            return raw.decode('latin-1')

    # ---------- booleans ----------
    @classmethod
    def normalize_boolean(cls, val) -> Optional[bool]:
        if val in cls.BOOL_TRUE:
            return True
        if val in cls.BOOL_FALSE:
            return False
        return None

    # ---------- dates ----------
    @staticmethod
    def parse_date(s: str | None) -> Optional[datetime.date]:
        if not s:
            return None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except ValueError:
                continue
        return None
