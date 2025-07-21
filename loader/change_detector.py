import logging

logger = logging.getLogger(__name__)

def has_file_changed(conn, file_hash):
    """
    Checks if the file hash exists in the loading_status table.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM loading_status WHERE file_hash = %s", (file_hash,))
        return cur.fetchone() is None

def mark_status(conn, state, doc_type, version, file_hash, status, error=None, rows=0):
    """
    Marks the loading status of a file in the database, updating if it already exists.
    This function performs an "upsert" operation.
    """
    try:
        with conn.cursor() as cur:
            # Using ON CONFLICT to perform an "upsert".
            # If the file_hash already exists, it updates the status, error, rows, and timestamp.
            # Otherwise, it inserts a new row.
            cur.execute("""
                INSERT INTO loading_status (file_hash, state, doc_type, version, status, error, rows, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (file_hash) DO UPDATE SET
                    state = EXCLUDED.state,
                    doc_type = EXCLUDED.doc_type,
                    version = EXCLUDED.version,
                    status = EXCLUDED.status,
                    error = EXCLUDED.error,
                    rows = EXCLUDED.rows,
                    last_updated = NOW();
            """, (file_hash, state, doc_type, version, status, error, rows))
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to mark status for file_hash {file_hash}: {e}")
        # Rollback in case of an error within this function
        conn.rollback()

