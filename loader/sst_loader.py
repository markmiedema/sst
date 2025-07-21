# D:\sst\loader\sst_loader.py

import io
import csv
import json
from pathlib import Path
from typing import Dict, List, Tuple
from psycopg2.extensions import connection

from parsers import coc as sst_certs
from parsers import lod as sst_lod
from parsers import tap as sst_tap

class SSTDatabaseLoader:
    def __init__(self, conn: connection):
        self.conn = conn
        self.lod = sst_lod.LODParser("LOD")
        # This line is now fixed with the correct class name
        self.tap = sst_tap.TAPPDFParser("TAP") 
        self.cert = sst_certs.CertParser("COC")

    def load_combined(self, csv_path: Path, doc_type: str,
                      state_code: str, state_name: str, version_hint: str):
        # Read the raw text from the file. Do NOT parse as JSON.
        raw_data = csv_path.read_text(encoding="utf-8-sig")

        # Use the version from the filename.
        # Let the specific parsers find the effective date from the file's content.
        version = version_hint
        eff_date = None  # This will be found by the parsers
        metadata = {}    # This will be found by the parsers

        # Insert the main document record and get the new ID
        doc_id = self._insert_doc(state_code, doc_type, version, eff_date, metadata)

        # Now, pass the raw data and the new ID to the specific loaders.
        # These loaders are responsible for parsing the CSV and inserting items.
        if doc_type == "LOD":
            self._load_lod(doc_id, raw_data, version, state_code, eff_date)
        elif doc_type == "TAP":
            self._load_tap(doc_id, raw_data)
        elif doc_type == "COC":
            self._load_cert(doc_id, raw_data, version)

        self.conn.commit()

    def _insert_doc(self, state: str, dt: str, version: str, eff, meta: Dict) -> int:
        with self.conn.cursor() as cur:
            cur.execute("""
              INSERT INTO document_versions
                (state_code, document_type_id, version, effective_date, metadata)
              VALUES (%s, (SELECT document_type_id FROM document_types WHERE document_type=%s),
                      %s, %s, %s)
              RETURNING document_version_id
            """, (state, dt, version, eff, json.dumps(meta)))
            result = cur.fetchone()
            if result:
                return result[0]
            raise Exception("Failed to insert document and retrieve ID")

    def _load_lod(self, doc_id: int, data: str, version: str, state: str, eff):
        items = self.lod.parse(data, version)
        rows: List[Tuple] = []
        for cat in items.values():
            for it in cat:
                rows.append((
                    doc_id, it["item_type"], it["code"], it["sst_definition"],
                    it["state_definition"], it["citation"], it["notes"]
                ))
        cols = ["document_version_id", "item_type", "code", "sst_definition",
                "state_definition", "citation", "notes"]
        self._copy("lod_items", rows, cols)

    def _load_cert(self, doc_id: int, data: str, version: str):
        items = self.cert.parse(data, version)
        rows: List[Tuple] = []
        for it in items:
            rows.append((doc_id, it["question_number"], it["question_text"], it["answer"]))
        cols = ["document_version_id", "question_number", "question_text", "answer"]
        self._copy("cert_items", rows, cols)

    def _load_tap(self, doc_id: int, data: str):
        items = self.tap.parse(data)
        rows: List[Tuple] = []
        for it in items:
            rows.append((
                doc_id, it["question_number"], it["question_text"],
                it["answer"], it["citation"], it["notes"]
            ))
        cols = ["document_version_id", "question_number", "question_text",
                "answer", "citation", "notes"]
        self._copy("tap_items", rows, cols)

    def _copy(self, table: str, rows: List, cols: List[str]):
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter='\t', lineterminator='\n')
        for row in rows:
            writer.writerow(['\\N' if x is None else x for x in row])
        buf.seek(0)
        with self.conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {table} ({','.join(cols)}) "
                "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                buf
            )