from __future__ import annotations
import re
import json
from pathlib import Path
from typing import Dict, List, Union
from .base import SSTDocumentParser
from .preprocessor import SSTDataPreprocessor
from .csv_parser import LODCSVParser

class LODParser(SSTDocumentParser):
    """Library‑of‑Definitions parser (CSV/JSON support)."""

    ADMIN_CODE_RANGE = (10000, 19999)
    HOLIDAY_CODES = {
        '20060','20070','20080','20090','20100','20110',
        '20120','20130','20140','20150','20160','20170',
        '20180','20190','20105',
    }
    _V2016 = re.compile(r'^v2016\.')

    def __init__(self, doc_type: str):
        self.doc_type = doc_type
        self.pre = SSTDataPreprocessor()
        self.csv_parser = LODCSVParser()
        super().__init__(self.pre)

    # public -----------------------------------------------------------------
    def parse(self, data: Union[str, Path], version: str) -> Dict[str, List[Dict]]:
        """Parse LOD data from CSV file path or JSON string."""
        # If data is a Path object or a string that looks like a file path
        if isinstance(data, Path):
            return self._parse_csv_file(data, version)
        elif isinstance(data, str):
            # Check if it's a file path string
            try:
                path = Path(data)
                if path.exists() and path.suffix.lower() == '.csv':
                    return self._parse_csv_file(path, version)
            except:
                pass
            
            # Otherwise try to parse as JSON string
            try:
                json_data = json.loads(data)
                if self._V2016.match(version):
                    return self._parse_2016_format(json_data)
                return self._parse_standard_format(json_data)
            except json.JSONDecodeError:
                # If it's not JSON and not a valid file path, raise error
                raise ValueError("LOD data must be either a CSV file path or valid JSON")
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

    # CSV parsing -------------------------------------------------------------
    def _parse_csv_file(self, file_path: Path, version: str) -> Dict[str, List[Dict]]:
        """Parse LOD data from CSV file."""
        items = self.csv_parser.parse(file_path)
        
        # CSV parser now returns the correct format, just need to normalize items
        normalized = {
            "admin_definitions": [],
            "product_definitions": [],
            "holiday_items": []
        }
        
        for category, category_items in items.items():
            for item in category_items:
                normalized_item = self._normalize_csv_item(item, category)
                if normalized_item:
                    normalized[category].append(normalized_item)
        
        return normalized
    
    def _normalize_csv_item(self, item: Dict, category: str) -> Dict:
        """Normalize CSV item to match expected format."""
        # Ensure all fields have proper defaults
        normalized = {
            'item_type': item.get('item_type', ''),
            'code': item.get('code', ''),
            'description': item.get('description', ''),
            'group_name': item.get('group_name', ''),
            'sst_definition': item.get('description', ''),  # Use description as SST definition
            'state_definition': item.get('state_definition', ''),
            'citation': item.get('reference', item.get('citation', '')),
            'notes': item.get('comment', item.get('notes', '')),
            'statute': item.get('reference', item.get('statute', '')),
            'comment': item.get('comment', item.get('notes', '')),
            'data': {}
        }
        
        # Add category-specific fields
        if category == 'admin_definitions':
            normalized['included'] = item.get('included')
            normalized['excluded'] = item.get('excluded')
        elif category == 'product_definitions':
            normalized['taxable'] = item.get('taxable')
            normalized['exempt'] = item.get('exempt')
        elif category == 'holiday_items':
            normalized['taxable'] = item.get('taxable')
            normalized['exempt'] = item.get('exempt')
            normalized['threshold'] = item.get('threshold')
        
        return normalized

    # JSON parsing (existing methods) -----------------------------------------
    def _parse_standard_format(self, d: Dict) -> Dict[str, List[Dict]]:
        return {
            "admin_definitions": [
                self._norm_admin(it) for it in d.get("admin_definitions", [])
            ],
            "product_definitions": [
                self._norm_prod(it) for it in d.get("product_definitions", [])
            ],
            "holiday_items": [
                self._norm_holiday(it)
                for it in d.get("sales_tax_holidays", {})
                       .get("holiday_items", [])
            ],
        }

    def _parse_2016_format(self, d: Dict) -> Dict[str, List[Dict]]:
        out = {"admin_definitions": [], "product_definitions": [], "holiday_items": []}
        for it in d.get("items", []) or d.get("definitions", []):
            code = str(it.get("code","")).strip()
            if not code:
                continue
            try:
                n = int(code)
                if self.ADMIN_CODE_RANGE[0] <= n <= self.ADMIN_CODE_RANGE[1]:
                    out["admin_definitions"].append(self._norm_admin(it)); continue
            except ValueError:
                pass
            if code in self.HOLIDAY_CODES:
                out["holiday_items"].append(self._norm_holiday(it))
            else:
                out["product_definitions"].append(self._norm_prod(it))
        return out

    # normalisers -------------------------------------------------------------
    def _norm_admin(self, it: Dict) -> Dict:
        p = self.pre
        return {
            "item_type": "admin_definition",
            "code": it.get("code"),
            "group_name": it.get("group"),
            "description": it.get("description"),
            "sst_definition": it.get("description", ""),
            "state_definition": it.get("state_definition", ""),
            "included": p.normalize_boolean(it.get("included")),
            "excluded": p.normalize_boolean(it.get("excluded")),
            "statute": it.get("statute"),
            "citation": it.get("citation", it.get("statute", "")),
            "comment": it.get("comment"),
            "notes": it.get("notes", it.get("comment", "")),
            "data": {k: v for k, v in it.items()
                     if k not in ("code","group","description","included",
                                  "excluded","statute","comment")},
        }

    def _norm_prod(self, it: Dict) -> Dict:
        p = self.pre
        return {
            "item_type": "product_definition",
            "code": it.get("code"),
            "group_name": it.get("group"),
            "description": it.get("description"),
            "sst_definition": it.get("description", ""),
            "state_definition": it.get("state_definition", ""),
            "taxable": p.normalize_boolean(it.get("taxable")),
            "exempt":  p.normalize_boolean(it.get("exempt")),
            "statute": it.get("statute"),
            "citation": it.get("citation", it.get("statute", "")),
            "comment": it.get("comment"),
            "notes": it.get("notes", it.get("comment", "")),
            "data": {k: v for k, v in it.items()
                     if k not in ("code","group","description","taxable",
                                  "exempt","statute","comment")},
        }

    def _norm_holiday(self, it: Dict) -> Dict:
        p = self.pre
        return {
            "item_type": "holiday_item",
            "code": it.get("code"),
            "description": it.get("description"),
            "sst_definition": it.get("description", ""),
            "state_definition": it.get("state_definition", ""),
            "taxable": p.normalize_boolean(it.get("taxable")),
            "exempt":  p.normalize_boolean(it.get("exempt")),
            "threshold": it.get("threshold"),
            "statute": it.get("statute"),
            "citation": it.get("citation", it.get("statute", "")),
            "comment": it.get("comment"),
            "notes": it.get("notes", it.get("comment", "")),
            "data": {k: v for k, v in it.items()
                     if k not in ("code","description","taxable",
                                  "exempt","threshold","statute","comment")},
        }