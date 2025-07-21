"""
main.py
--------

Load ONE raw SST document (CSV/JSON) into Supabase Postgres.

• Credentials come from .env  (PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE).
• The file path, document‑type, and state code can be passed on the CLI:

    python main.py  tm  AR  "D:/DataLake/raw/sst/tm/state=AR/tm_AR_v2024.0_20250706T220321.csv"

If you omit arguments it falls back to the hard‑coded EXAMPLE_FILE below.
"""

from pathlib import Path
import sys, hashlib
from dotenv import load_dotenv
from config import get_connection          # helper that reads .env
from loader.sst_loader import SSTDatabaseLoader
from loader.change_detector import mark_status
from config import get_doc_type, get_state_name

# --------------------------------------------------------------------------- #
# 1.  CLI / defaults
# --------------------------------------------------------------------------- #
EXAMPLE_FILE = Path(r"D:\DataLake\raw\sst\tm\state=AR\tm_AR_v2024.0_20250706T220321.csv")
if len(sys.argv) == 4:
    raw_type, state, file_path = sys.argv[1:4]
    doc_type = get_doc_type(raw_type)
    state_code = state.upper()
    FILE = Path(file_path)
else:
    raw_type  = "tm"
    state_code = "AR"
    doc_type  = "LOD"
    FILE      = EXAMPLE_FILE

# --------------------------------------------------------------------------- #
# 2.  Lookup state name from config
# --------------------------------------------------------------------------- #
state_name = get_state_name(state_code)

# --------------------------------------------------------------------------- #
# 3.  Connect & load
# --------------------------------------------------------------------------- #
load_dotenv()                    # automatically reads .env at project root
conn   = get_connection()        # psycopg2 connection
loader = SSTDatabaseLoader(conn)

def sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()

version_hint = FILE.stem.split("_")[2]      # eg  v2024.0
file_hash    = sha256(FILE)

try:
    mark_status(conn, state_code, doc_type, version_hint, file_hash, "started")
    loader.load_combined(FILE, doc_type, state_code, state_name, version_hint)
    mark_status(conn, state_code, doc_type, version_hint, file_hash,
                "completed", rows=1)
    print(f"✓ Loaded {doc_type} {state_code} {version_hint}")
except Exception as exc:
    mark_status(conn, state_code, doc_type, version_hint, file_hash,
                "failed", error=str(exc))
    raise
finally:
    conn.close()
