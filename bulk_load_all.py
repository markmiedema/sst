"""
bulk_load_all.py
----------------

Recursively load EVERY raw SST CSV in the lake:

D:/DataLake/raw/sst/{cc|tap|tm}/state=XX/*.csv

Run with the venv active:
    python bulk_load_all.py
"""

from pathlib import Path
import json, hashlib, sys
from dotenv import load_dotenv
from loader.db import get_connection
from loader.sst_loader import SSTDatabaseLoader
from loader.change_detector import mark_status

# --------------------------------------------------------------------------- #
RAW_ROOT = Path(r"D:\DataLake\raw\sst")     # <— adjust if your root is different
DOC_MAP  = {"cc": "CERT", "tap": "TAP", "tm": "LOD"}

STATE_NAMES = json.load(open("config/state_names.json", encoding="utf-8"))
load_dotenv()                                # read .env
conn   = get_connection()
loader = SSTDatabaseLoader(conn)

def sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()

for folder_key, doc_type in DOC_MAP.items():
    base = RAW_ROOT / folder_key
    if not base.exists():
        print(f"⚠  {base} missing – skipped")
        continue

    for state_dir in base.glob("state=*"):
        state_code = state_dir.name.split("=", 1)[1].upper()
        state_name = STATE_NAMES.get(state_code, state_code)

        for f in state_dir.glob("*.csv"):
            ver = f.stem.split("_")[2]                    # v2024.0
            h   = sha256(f)
            try:
                mark_status(conn, state_code, doc_type, ver, h, "started")
                loader.load_combined(f, doc_type, state_code, state_name)
                mark_status(conn, state_code, doc_type, ver, h,
                            "completed", rows=1)
                print(f"✓ {doc_type} {state_code} {ver}")
            except Exception as e:
                mark_status(conn, state_code, doc_type, ver, h,
                            "failed", error=str(e))
                print(f"✗ {doc_type} {state_code} {ver}: {e}", file=sys.stderr)

conn.close()
print("🚀  bulk load finished")
