"""
bulk_load_all.py
----------------

Recursively load EVERY raw SST CSV in the lake using configuration.

Run with the venv active:
    python bulk_load_all.py
"""

from pathlib import Path
import json
import hashlib
import sys
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import config, get_connection
from loader.sst_loader import SSTDatabaseLoader
from loader.change_detector import mark_status
from loader.logging_config import setup_logging

# Set up logging
loggers = setup_logging(config.loading.log_dir, config.loading.log_level)
logger = loggers['loader']

# --------------------------------------------------------------------------- #
# Configuration-based settings
RAW_ROOT = config.loading.data_lake_path
DOC_MAP = {"cc": "CERT", "tap": "TAP", "tm": "LOD"}
MAX_WORKERS = 4  # Configurable parallel workers

# Load state names
try:
    STATE_NAMES = json.load(open("config/state_names.json", encoding="utf-8"))
except FileNotFoundError:
    logger.error("state_names.json not found - using state codes as names")
    STATE_NAMES = {}

def sha256(p: Path) -> str:
    """Calculate SHA256 hash of a file."""
    return hashlib.sha256(p.read_bytes()).hexdigest()

def process_file(file_path: Path, doc_type: str, state_code: str, state_name: str) -> tuple:
    """Process a single file and return status."""
    version = file_path.stem.split("_")[2]  # Extract version (e.g., v2024.0)
    file_hash = sha256(file_path)
    
    # Get a fresh connection for this thread
    conn = get_connection()
    loader = SSTDatabaseLoader(conn)
    
    try:
        mark_status(conn, state_code, doc_type, version, file_hash, "started")
        loader.load_combined(file_path, doc_type, state_code, state_name, version)
        mark_status(conn, state_code, doc_type, version, file_hash,
                    "completed", rows=1)
        logger.info(f"✓ Loaded {doc_type} {state_code} {version}")
        conn.close()
        return (True, doc_type, state_code, version, None)
        
    except Exception as e:
        mark_status(conn, state_code, doc_type, version, file_hash,
                    "failed", error=str(e))
        logger.error(f"✗ Failed {doc_type} {state_code} {version}: {e}")
        conn.close()
        return (False, doc_type, state_code, version, str(e))

def main():
    """Main bulk loading function."""
    start_time = datetime.now()
    logger.info(f"🚀 Starting bulk load from: {RAW_ROOT}")
    
    # Collect all files to process
    files_to_process = []
    
    for folder_key, doc_type in DOC_MAP.items():
        base = RAW_ROOT / folder_key
        if not base.exists():
            logger.warning(f"⚠  {base} missing – skipped")
            continue

        for state_dir in base.glob("state=*"):
            state_code = state_dir.name.split("=", 1)[1].upper()
            state_name = STATE_NAMES.get(state_code, state_code)

            for f in state_dir.glob("*.csv"):
                files_to_process.append((f, doc_type, state_code, state_name))
    
    if not files_to_process:
        logger.error("No files found to process!")
        return
    
    logger.info(f"Found {len(files_to_process)} files to process")
    
    # Process files with parallel execution
    success_count = 0
    failure_count = 0
    results = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(process_file, f, dt, sc, sn): (f, dt, sc)
            for f, dt, sc, sn in files_to_process
        }
        
        # Process completed tasks
        for future in as_completed(future_to_file):
            file_info = future_to_file[future]
            try:
                success, doc_type, state_code, version, error = future.result()
                if success:
                    success_count += 1
                else:
                    failure_count += 1
                results.append((success, doc_type, state_code, version, error))
            except Exception as e:
                failure_count += 1
                logger.error(f"Unexpected error processing {file_info[0]}: {e}")
    
    # Summary report
    elapsed = datetime.now() - start_time
    logger.info("=" * 50)
    logger.info(f"🏁 Bulk load completed in {elapsed}")
    logger.info(f"✅ Successful: {success_count}")
    logger.info(f"❌ Failed: {failure_count}")
    
    # Show failed files for investigation
    if failure_count > 0:
        logger.info("\nFailed loads:")
        for success, doc_type, state_code, version, error in results:
            if not success:
                logger.error(f"  - {doc_type} {state_code} {version}: {error}")
    
    # Exit with appropriate code
    sys.exit(0 if failure_count == 0 else 1)

if __name__ == "__main__":
    main()