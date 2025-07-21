#!/usr/bin/env python3
"""
verify_load.py - Verify that test data was loaded successfully
"""

from config import get_connection
from datetime import datetime

def verify_test_load():
    """Check if test data was loaded successfully."""
    conn = get_connection()
    
    print("🔍 Verifying Test Load")
    print("=" * 50)
    
    try:
        with conn.cursor() as cur:
            # Check document_versions
            cur.execute("""
                SELECT state_code, document_type, version, effective_date, loaded_at
                FROM document_versions dv
                JOIN document_types dt ON dv.document_type_id = dt.document_type_id
                ORDER BY loaded_at DESC
                LIMIT 5
            """)
            
            docs = cur.fetchall()
            
            if not docs:
                print("❌ No documents found in database")
                return False
            
            print(f"✅ Found {len(docs)} recent document(s):")
            for state, doc_type, version, eff_date, loaded_at in docs:
                print(f"   {state} - {doc_type} - {version}")
                print(f"   Effective: {eff_date}, Loaded: {loaded_at}")
                print()
            
            # Check item counts
            print("📊 Item Counts:")
            for table in ['lod_items', 'cert_items', 'tap_items']:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                if count > 0:
                    print(f"   ✅ {table}: {count:,} items")
                    
                    # Show sample items
                    cur.execute(f"""
                        SELECT state_code, code, 
                               CASE 
                                   WHEN description IS NOT NULL THEN LEFT(description, 50)
                                   ELSE 'No description'
                               END as description_preview
                        FROM {table}
                        LIMIT 3
                    """)
                    samples = cur.fetchall()
                    for state, code, desc in samples:
                        print(f"      • {state} - {code}: {desc}...")
                else:
                    print(f"   ℹ️  {table}: 0 items")
            
            # Check loading_status
            print("\n📈 Loading Status:")
            
            # First check if the started_at column exists
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'loading_status' 
                AND column_name IN ('started_at', 'created_at')
                LIMIT 1
            """)
            timestamp_col = cur.fetchone()
            
            if timestamp_col:
                cur.execute("""
                    SELECT status, COUNT(*) as count
                    FROM loading_status
                    GROUP BY status
                    ORDER BY count DESC
                """)
                
                statuses = cur.fetchall()
                if statuses:
                    for status, count in statuses:
                        emoji = "✅" if status == "completed" else "❌" if status == "failed" else "🔄"
                        print(f"   {emoji} {status}: {count}")
                else:
                    print("   No loading status records yet")
            else:
                print("   ⚠️  Loading status table missing timestamp column")
                
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error verifying load: {e}")
        conn.close()
        return False

if __name__ == "__main__":
    verify_test_load()