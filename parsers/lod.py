from __future__ import annotations
import re
from typing import Dict, List
from .base import SSTDocumentParser
from .preprocessor import SSTDataPreprocessor

class LODParser(SSTDocumentParser):
    """Library‑of‑Definitions parser (CSV/JSON already materialised)."""

    ADMIN_CODE_RANGE = (10000, 19999)
    HOLIDAY_CODES = {
        '20060','20070','20080','20090','20100','20110',
        '20120','20130','20140','20150','20160','20170',
        '20180','20190','20105',
    }
    _V2016 = re.compile(r'^v2016\.')

    # public -----------------------------------------------------------------
    def parse(self, data: Dict, version: str) -> Dict[str, List[Dict]]:
        if self._V2016.match(version):
            return self._parse_2016_format(data)
        return self._parse_standard_format(data)

    # internal ----------------------------------------------------------------
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
            "included": p.normalize_boolean(it.get("included")),
            "excluded": p.normalize_boolean(it.get("excluded")),
            "statute": it.get("statute"),
            "comment": it.get("comment"),
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
            "taxable": p.normalize_boolean(it.get("taxable")),
            "exempt":  p.normalize_boolean(it.get("exempt")),
            "statute": it.get("statute"),
            "comment": it.get("comment"),
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
            "taxable": p.normalize_boolean(it.get("taxable")),
            "exempt":  p.normalize_boolean(it.get("exempt")),
            "threshold": it.get("threshold"),
            "statute": it.get("statute"),
            "comment": it.get("comment"),
            "data": {k: v for k, v in it.items()
                     if k not in ("code","description","taxable",
                                  "exempt","threshold","statute","comment")},
        }
