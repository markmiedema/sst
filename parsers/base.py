from __future__ import annotations
import re
from typing import Dict, List
from .preprocessor import SSTDataPreprocessor

# -------------------- Robust row‑level wrapper --------------------
class RobustParser:
    """Wrap any per‑row parser; abort only if error‑rate > threshold."""

    def __init__(self, parse_row_fn, error_threshold: float = 0.10):
        self.parse_row = parse_row_fn
        self.error_threshold = error_threshold
        self.errors: List[Dict] = []

    def parse_with_recovery(self, rows) -> List[Dict]:
        total = len(rows)
        parsed = []
        for idx, row in enumerate(rows):
            try:
                parsed.append(self.parse_row(row))
            except Exception as e:
                self.errors.append(
                    {"row": idx, "error": str(e), "data": row}
                )
        if total and len(self.errors) / total > self.error_threshold:
            raise RuntimeError("Error‑rate exceeded threshold")
        return parsed

# -------------------- Base document parser -----------------------
class SSTDocumentParser:
    def __init__(self, pre: SSTDataPreprocessor):
        self.pre = pre

    # ---- metadata date clean‑up
    def parse_metadata(self, data: Dict) -> Dict:
        meta = data.get("metadata", {})
        for k in ("Effective Date", "Published Date", "Revised Date"):
            if k in meta:
                d = self.pre.parse_date(meta[k])
                if d:
                    meta[k] = d.isoformat()
        return meta

    # ---- version string helper
    _VER_RE = re.compile(r"^v\d{4}\.\d+$")

    def detect_version(self, data: Dict) -> str:
        v = data.get("metadata", {}).get("Version", "")
        if self._VER_RE.match(v):
            return v
        return f"{data.get('state_code','XX')}_unknown"
