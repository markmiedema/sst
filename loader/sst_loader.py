# D:\sst\loader\sst_loader.py

import io
import csv
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import date, datetime
from psycopg2.extensions import connection

from parsers import coc as sst_certs
from parsers import lod as sst_lod
from parsers import tap as sst_tap
from parsers.preprocessor import SSTDataPreprocessor

class SSTDatabaseLoader:
    def __init__(self, conn: connection):
        self.conn = conn
        self.lod = sst_lod.LODParser("LOD")
        self.tap = sst_tap.TAPPDFParser()
        self.cert = sst_certs.CertParser("COC")
        self.preprocessor = SSTDataPreprocessor()

    def load_combined(self, csv_path: Path, doc_type: str,
                      state_code: str, state_name: str, version_hint: str):
        """Load a document with proper transaction handling."""
        try:
            # Start transaction
            with self.conn:
                # For CSV files, we need to pass the path to parsers for encoding detection
                # Extract metadata and effective date
                metadata, eff_date = self._extract_metadata_from_file(csv_path, version_hint)
                
                # Use version from filename as primary source
                version = version_hint
                
                # Insert the main document record and get the new ID
                doc_id = self._insert_doc(state_code, doc_type, version, eff_date, metadata)
                
                # Pass the file path to the specific loaders for CSV files
                if doc_type == "LOD":
                    self._load_lod(doc_id, csv_path, version, state_code, eff_date)
                elif doc_type == "TAP":
                    self._load_tap(doc_id, csv_path)
                elif doc_type == "CERT" or doc_type == "COC":
                    self._load_cert(doc_id, csv_path, version)
                    
        except Exception as e:
            # Transaction will auto-rollback on exception
            raise Exception(f"Failed to load {doc_type} for {state_code}: {str(e)}")

    def _extract_metadata_from_file(self, file_path: Path, version: str) -> Tuple[Dict, Optional[date]]:
        """Extract metadata from CSV file."""
        metadata = {
            "filename": file_path.name,
            "version": version,
            "loaded_at": datetime.now().isoformat()
        }
        
        # Try to extract date from filename
        # Format: tm_AR_v2024.0_20250706T220321.csv
        parts = file_path.stem.split('_')
        eff_date = None
        
        if len(parts) >= 4:
            try:
                # Extract date from timestamp part (20250706T220321)
                timestamp_str = parts[3]
                date_part = timestamp_str.split('T')[0]
                if len(date_part) == 8:  # YYYYMMDD
                    year = int(date_part[0:4])
                    month = int(date_part[4:6])
                    day = int(date_part[6:8])
                    eff_date = date(year, month, day)
            except (ValueError, IndexError):
                pass
        
        # Default to current date if no effective date found
        if eff_date is None:
            eff_date = date.today()
            
        return metadata, eff_date

    def _insert_doc(self, state: str, dt: str, version: str, eff: Optional[date], meta: Dict) -> int:
        """Insert document version with proper effective date."""
        with self.conn.cursor() as cur:
            # Ensure we have an effective date
            if eff is None:
                eff = date.today()
                
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

    def _load_lod(self, doc_id: int, file_path: Path, version: str, state: str, eff: Optional[date]):
        """Load Library of Definitions items from CSV file."""
        # Parse the CSV file directly
        items = self.lod.parse(file_path, version)
        
        rows: List[Tuple] = []
        for cat_name, cat_items in items.items():
            for it in cat_items:
                # Extract fields with defaults
                rows.append((
                    doc_id,
                    it.get("item_type", cat_name.rstrip('s')),  # Remove plural 's'
                    it.get("code", ""),
                    it.get("description", ""),
                    it.get("taxable"),
                    it.get("exempt"),
                    it.get("included"),
                    it.get("excluded"),
                    it.get("threshold"),
                    it.get("rate"),
                    it.get("statute", ""),
                    it.get("citation", ""),
                    it.get("comment", ""),
                    json.dumps(it.get("data", {})),
                    state,
                    eff or date.today()
                ))
        
        cols = ["document_version_id", "item_type", "code", "description",
                "taxable", "exempt", "included", "excluded", "threshold", "rate",
                "statute", "citation", "comment", "data", "state_code", "effective_date"]
        self._copy("lod_items", rows, cols)

    def _load_cert(self, doc_id: int, file_path: Path, version: str):
        """Load Certificate of Compliance items from CSV file."""
        items = self.cert.parse(file_path, version)
        
        # Get state_code and effective_date from document_versions
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT state_code, effective_date 
                FROM document_versions 
                WHERE document_version_id = %s
            """, (doc_id,))
            state_code, eff_date = cur.fetchone()
        
        rows: List[Tuple] = []
        for it in items:
            # Map question_number to section/code structure
            section = it.get("section", "General")
            code = it.get("question_number", "")
            
            rows.append((
                doc_id,
                section,
                code,
                it.get("topic", ""),
                it.get("question_text", ""),
                it.get("compliance_met"),
                it.get("citation", ""),
                it.get("effective_dates", ""),
                it.get("notes", ""),
                json.dumps({"answer": it.get("answer", "")}),  # Store answer in data field
                state_code,
                eff_date
            ))
        
        cols = ["document_version_id", "section", "code", "topic", "description",
                "compliance_met", "citation", "effective_dates", "notes", "data",
                "state_code", "effective_date"]
        self._copy("cert_items", rows, cols)

    def _load_tap(self, doc_id: int, file_path: Path):
        """Load Tax Administration Practices items from CSV file."""
        items = self.tap.parse(file_path)
        
        # Get state_code and effective_date from document_versions
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT state_code, effective_date 
                FROM document_versions 
                WHERE document_version_id = %s
            """, (doc_id,))
            state_code, eff_date = cur.fetchone()
        
        rows: List[Tuple] = []
        for it in items:
            group_name = it.get("group_name", "General")
            code = it.get("question_number", "")
            
            rows.append((
                doc_id,
                group_name,
                it.get("subgroup", ""),
                code,
                it.get("question_text", ""),
                it.get("compliance_met"),
                it.get("citation", ""),
                it.get("notes", ""),
                json.dumps({"answer": it.get("answer", "")}),
                state_code,
                eff_date
            ))
        
        cols = ["document_version_id", "group_name", "subgroup", "code", "description",
                "compliance_met", "citation", "comment", "data",
                "state_code", "effective_date"]
        self._copy("tap_items", rows, cols)

    def _copy(self, table: str, rows: List[Tuple], cols: List[str]):
        """Bulk insert rows using COPY for performance."""
        if not rows:
            return
            
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