#!/usr/bin/env python3
"""
db_health_check.py - Comprehensive database health check
"""

import sys
from datetime import datetime
from config import config, get_connection
import psycopg2

def check_database_connection():
    """Test basic database connectivity."""
    print("🔍 Checking database connection...")
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print(f"✅ Connected to PostgreSQL")
            print(f"   Version: {version}")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def check_schema_integrity():
    """Verify all required tables exist."""
    print("\n🔍 Checking schema integrity...")
    required_tables = [
        'states', 'document_types', 'document_versions',
        'lod_items', 'cert_items', 'tap_items', 'loading_status'
    ]
    
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
            """)
            existing_tables = {row[0] for row in cur.fetchall()}
        
        missing_tables = []
        for table in required_tables:
            if table in existing_tables:
                print(f"✅ Table '{table}' exists")
            else:
                print(f"❌ Table '{table}' is MISSING")
                missing_tables.append(table)
        
        conn.close()
        return len(missing_tables) == 0
        
    except Exception as e:
        print(f"❌ Schema check failed: {e}")
        return False

def check_data_statistics():
    """Get current data statistics."""
    print("\n📊 Data Statistics:")
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Document versions count
            cur.execute("SELECT COUNT(*) FROM document_versions")
            doc_count = cur.fetchone()[0]
            print(f"   Document versions: {doc_count:,}")
            
            # Items count by type
            for table in ['lod_items', 'cert_items', 'tap_items']:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"   {table}: {count:,}")
            
            # Recent loading activity
            cur.execute("""
                SELECT status, COUNT(*) 
                FROM loading_status 
                WHERE started_at > NOW() - INTERVAL '7 days'
                GROUP BY status
            """)
            
            print("\n📈 Recent loading activity (last 7 days):")
            for status, count in cur.fetchall():
                print(f"   {status}: {count}")
            
            # States with data
            cur.execute("""
                SELECT COUNT(DISTINCT state_code) as state_count,
                       COUNT(DISTINCT document_type_id) as doc_type_count
                FROM document_versions
            """)
            state_count, doc_type_count = cur.fetchone()
            print(f"\n🏛️  Coverage:")
            print(f"   States with data: {state_count}")
            print(f"   Document types: {doc_type_count}")
            
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Statistics check failed: {e}")
        return False

def check_configuration():
    """Verify configuration is valid."""
    print("\n⚙️  Configuration Check:")
    try:
        print(f"✅ Database host: {config.database.host}")
        print(f"✅ Database name: {config.database.database}")
        print(f"✅ Data lake path: {config.loading.data_lake_path}")
        print(f"✅ Log directory: {config.loading.log_dir}")
        
        # Check if data lake path exists
        if config.loading.data_lake_path.exists():
            print(f"✅ Data lake path exists")
            # Count available files
            csv_count = len(list(config.loading.data_lake_path.rglob("*.csv")))
            print(f"   Found {csv_count} CSV files")
        else:
            print(f"❌ Data lake path does not exist!")
            
        return True
    except Exception as e:
        print(f"❌ Configuration check failed: {e}")
        return False

def run_sample_query():
    """Run a sample query to test functionality."""
    print("\n🧪 Running sample query...")
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Try to get a sample state
            cur.execute("""
                SELECT DISTINCT state_code, document_type, version
                FROM document_versions dv
                JOIN document_types dt ON dv.document_type_id = dt.document_type_id
                LIMIT 5
            """)
            
            results = cur.fetchall()
            if results:
                print("✅ Sample data:")
                for state, doc_type, version in results:
                    print(f"   {state} - {doc_type} - {version}")
            else:
                print("ℹ️  No data loaded yet")
                
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Sample query failed: {e}")
        return False

def main():
    """Run all health checks."""
    print("🏥 SST Database Health Check")
    print("=" * 50)
    print(f"Timestamp: {datetime.now()}")
    
    checks = [
        ("Database Connection", check_database_connection),
        ("Schema Integrity", check_schema_integrity),
        ("Configuration", check_configuration),
        ("Data Statistics", check_data_statistics),
        ("Sample Query", run_sample_query)
    ]
    
    all_passed = True
    
    for check_name, check_func in checks:
        if not check_func():
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("✅ All health checks passed!")
        sys.exit(0)
    else:
        print("❌ Some health checks failed. Please investigate.")
        sys.exit(1)

if __name__ == "__main__":
    main()