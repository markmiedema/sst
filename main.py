import argparse
import hashlib
import logging
import os
import sys

import psycopg2

from config import config
from loader.change_detector import has_file_changed, mark_status
from loader.sst_loader import SSTDatabaseLoader
from loader.logging_config import setup_logging

# Set up logging for the application
setup_logging()
logger = logging.getLogger(__name__)

def get_file_hash(file_path):
    """
    Computes the SHA256 hash of a file.
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def main():
    """
    Main function to process a single SST file.
    """
    parser = argparse.ArgumentParser(description="Load a single SST file into the database.")
    parser.add_argument("--file-path", required=True, help="The full path to the document to test.")
    parser.add_argument("--state", required=True, help="The two-letter state code (e.g., AR).")
    parser.add_argument("--document-type", required=True, choices=['CERT', 'TAP', 'LOD'], help="The type of document.")
    parser.add_argument("--effective-date", required=True, help="The effective date of the document in YYYY-MM-DD format.")
    args = parser.parse_args()

    file_path = args.file_path
    state_code = args.state
    doc_type = args.document_type
    effective_date = args.effective_date
    version_hint = os.path.basename(file_path)

    conn = None
    try:
        conn = psycopg2.connect(config.database.connection_string)
        file_hash = get_file_hash(file_path)

        if not has_file_changed(conn, file_hash):
            logger.info(f"File {file_path} has not changed and will be skipped.")
            sys.exit(0)

        loader = SSTDatabaseLoader(conn)
        
        try:
            # Main document loading logic
            if doc_type == 'CERT':
                rows_loaded = loader.load_cert_document(file_path, state_code, effective_date, version_hint)
            elif doc_type == 'TAP':
                rows_loaded = loader.load_tap_document(file_path, state_code, effective_date, version_hint)
            elif doc_type == 'LOD':
                rows_loaded = loader.load_lod_document(file_path, state_code, effective_date, version_hint)
            else:
                raise ValueError("Unsupported document type")

            # Mark as completed if successful
            mark_status(conn, state_code, doc_type, version_hint, file_hash, "completed", rows=rows_loaded)
            logger.info(f"Successfully loaded {rows_loaded} rows from {file_path}.")

        except Exception as exc:
            # If an error occurs, rollback the transaction to allow for error logging
            if conn:
                conn.rollback()
            logger.error(f"Failed to process file {file_path}: {exc}")
            mark_status(conn, state_code, doc_type, version_hint, file_hash, "failed", error=str(exc))
            raise

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
