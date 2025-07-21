from __future__ import annotations
import csv, io, json
from pathlib import Path
import psycopg2
from typing import List, Tuple
from parsers.preprocessor import SSTDataPreprocessor
from parsers.lod import LODParser

class SSTDatabaseLoader:
    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn
        pre = SSTDataPreprocessor()
        self.pre = pre
        self.lod = LODParser(pre)

    # ------------- public ---------------
    def load_combined(self, csv_path: Path, doc_type: str,
                      state_code: str, state_name: str):
        # for demo we only handle LOD CSV already split by version
        data = json.loads(csv_path.read_text(encoding="utf-8"))
        metadata = data.get("metadata", {})
        version = metadata.get("Version","v0000.0")
        eff      = self.pre.parse_date(metadata.get("Effective Date",""))

        doc_id = self._insert_doc(state_code, doc_type, version, eff, metadata)
        if doc_type == "LOD":
            self._load_lod(doc_id, data, version, state_code, eff)
        self.conn.commit()

    # ------------- helpers --------------
    def _insert_doc(self, state, dt, version, eff, meta) -> int:
        with self.conn.cursor() as cur:
            cur.execute("""
              INSERT INTO document_versions
                (state_code, document_type_id, version, effective_date, metadata)
              VALUES (%s, (SELECT document_type_id FROM document_types WHERE document_type=%s),
                      %s, %s, %s)
              RETURNING document_version_id
            """, (state, dt, version, eff, json.dumps(meta)))
            return cur.fetchone()[0]

    def _load_lod(self, doc_id, data, version, state, eff):
        items = self.lod.parse(data, version)
        rows: List[Tuple] = []
        for cat in items.values():
            for it in cat:
                rows.append((
                    doc_id, it["item_type"], it["code"], it.get("group_name"),
                    it.get("description"), it.get("taxable"), it.get("exempt"),
                    it.get("included"), it.get("excluded"), it.get("threshold"),
                    it.get("rate"), it.get("statute"), it.get("citation"),
                    it.get("comment"), json.dumps(it.get("data")), state, eff
                ))
        self._copy("lod_items", rows, [
            "document_version_id","item_type","code","group_name","description",
            "taxable","exempt","included","excluded","threshold","rate",
            "statute","citation","comment","data","state_code","effective_date"
        ])

    def _copy(self, table, rows, cols):
        buf = io.StringIO()
        w = csv.writer(buf, delimiter='\t', lineterminator='\n')
        for r in rows:
            w.writerow(['\\N' if x is None else x for x in r])
        buf.seek(0)
        with self.conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {table} ({','.join(cols)}) "
                "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                buf)
