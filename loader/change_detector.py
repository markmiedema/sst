import hashlib
from pathlib import Path

def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()

def mark_status(conn, state, doc_type, version, file_hash,
                status, error=None, rows=None):
    with conn.cursor() as cur:
        cur.execute("""
          INSERT INTO loading_status
                (state_code, document_type, version, file_hash,
                 status, error_message, row_count,
                 completed_at)
          VALUES (%s,%s,%s,%s,%s,%s,%s,
                  CASE WHEN %s IN ('completed','failed') THEN now() END)
        """, (state, doc_type, version, file_hash,
              status, error, rows, status))
