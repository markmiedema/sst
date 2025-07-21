#!/usr/bin/env python3
"""
manage.py - Management script for common SST operations.

Usage:
    python manage.py load <file_path>                # Load single file
    python manage.py bulk <directory>                # Bulk load directory
    python manage.py status                          # Show system status
    python manage.py cleanup                         # Cleanup old logs/data
    python manage.py schema --init                   # Initialize database schema
    python manage.py schema --upgrade                # Upgrade database schema
    python manage.py export <state> <doc_type>      # Export data to CSV
"""

import argparse
import json
import sys
import shutil
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config import config, get_connection
from loader.sst_loader import SSTDatabaseLoader
from monitoring.dashboard import LoadingMonitor
from bulk_load_all import main as bulk_load_main


def load_single_file(file_path: str):
    """Load a single file into the database."""
    file_path = Path(file_path)
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return False
    
    # Parse file info from path
    # Expected format: /data_lake/raw/sst/{folder}/state={code}/file_{version}_{date}.csv
    try:
        parts = file_path.parts
        folder = None
        state_code = None
        
        # Find folder and state
        for i, part in enumerate(parts):
            if part in ['tm', 'tap', 'cc']:
                folder = part
            elif part.startswith('state='):
                state_code = part.split('=')[1]
                break
        
        if not folder or not state_code:
            print(f"❌ Could not parse folder/state from path: {file_path}")
            return False
        
        # Map folder to document type
        doc_type_map = {'tm': 'LOD', 'tap': 'TAP', 'cc': 'CERT'}
        doc_type = doc_type_map.get(folder)
        
        if not doc_type:
            print(f"❌ Unknown document type for folder: {folder}")
            return False
        
        # Extract version from filename
        filename = file_path.stem
        version = "unknown"
        if '_' in filename:
            parts = filename.split('_')
            if len(parts) >= 2:
                version = parts[1]
        
        print(f"📄 Loading {doc_type} file for {state_code} (version: {version})")
        
        conn = get_connection()
        loader = SSTDatabaseLoader(conn)
        
        # Get state name from config or database
        state_names = {
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
            'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
            'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
            'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
            'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
            'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
            'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
            'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
            'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
            'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
            'WI': 'Wisconsin', 'WY': 'Wyoming'
        }
        state_name = state_names.get(state_code, state_code)
        
        loader.load_combined(file_path, doc_type, state_code, state_name, version)
        conn.close()
        
        print(f"✅ Successfully loaded {file_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return False


def bulk_load_directory(directory: str):
    """Bulk load all files in a directory."""
    directory = Path(directory)
    if not directory.exists():
        print(f"❌ Directory not found: {directory}")
        return False
    
    print(f"📁 Starting bulk load from: {directory}")
    
    try:
        # Use existing bulk_load_all functionality
        sys.argv = ['bulk_load_all.py', str(directory)]
        bulk_load_main()
        print(f"✅ Bulk load completed")
        return True
    except Exception as e:
        print(f"❌ Bulk load failed: {e}")
        return False


def show_system_status():
    """Show comprehensive system status."""
    print("🏛️  SST System Status")
    print("=" * 50)
    
    try:
        conn = get_connection()
        monitor = LoadingMonitor(conn)
        
        # Recent activity
        summary = monitor.get_loading_summary(days_back=7)
        print(f"\n📊 Recent Activity (7 days)")
        print("-" * 30)
        if summary['summary']:
            for status, data in summary['summary'].items():
                print(f"{status}: {data['count']} loads ({data['states']} states)")
        else:
            print("No recent activity")
        
        # Failed loads
        failed = monitor.get_failed_loads()
        print(f"\n❌ Failed Loads: {len(failed)}")
        if failed:
            print("Recent failures:")
            for load in failed[:5]:  # Show first 5
                print(f"  • {load['state_code']} {load['document_type']} (attempts: {load['attempt_count']})")
        
        # Performance
        perf = monitor.get_loading_performance()
        if perf.get('by_document_type'):
            print(f"\n⚡ Performance (average load time)")
            print("-" * 30)
            for doc_type, metrics in perf['by_document_type'].items():
                print(f"{doc_type}: {metrics['avg_seconds']:.1f}s")
        
        # Database stats
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    'document_versions' as table_name, COUNT(*) as count
                FROM document_versions
                UNION ALL
                SELECT 'lod_items', COUNT(*) FROM lod_items
                UNION ALL
                SELECT 'cert_items', COUNT(*) FROM cert_items
                UNION ALL
                SELECT 'tap_items', COUNT(*) FROM tap_items
            """)
            
            print(f"\n📊 Database Contents")
            print("-" * 30)
            for row in cur.fetchall():
                print(f"{row[0]}: {row[1]:,} records")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error getting system status: {e}")


def cleanup_old_data():
    """Cleanup old logs and temporary data."""
    print("🧹 Cleaning up old data...")
    
    # Cleanup logs older than 30 days
    log_dir = config.loading.log_dir
    if log_dir.exists():
        cutoff_date = datetime.now() - timedelta(days=30)
        cleaned_files = 0
        
        for log_file in log_dir.glob("*.log*"):
            if log_file.stat().st_mtime < cutoff_date.timestamp():
                log_file.unlink()
                cleaned_files += 1
        
        print(f"🗑️  Removed {cleaned_files} old log files")
    
    # Optional: cleanup old loading status records
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM loading_status 
                WHERE started_at < NOW() - INTERVAL '90 days'
                AND status IN ('completed', 'failed')
            """)
            deleted_count = cur.rowcount
            conn.commit()
        conn.close()
        
        print(f"🗑️  Removed {deleted_count} old loading status records")
        
    except Exception as e:
        print(f"⚠️  Could not cleanup database records: {e}")


def export_data(state_code: str, doc_type: str, output_file: str = None):
    """Export data to CSV format."""
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"export_{state_code}_{doc_type}_{timestamp}.csv"
    
    print(f"📤 Exporting {state_code} {doc_type} data to {output_file}")
    
    try:
        conn = get_connection()
        
        # Map doc_type to table
        table_map = {'LOD': 'lod_items', 'CERT': 'cert_items', 'TAP': 'tap_items'}
        table = table_map.get(doc_type.upper())
        
        if not table:
            print(f"❌ Unknown document type: {doc_type}")
            return False
        
        with conn.cursor() as cur:
            # Get current data for the state
            cur.execute(f"""
                SELECT t.*, dv.version, dv.effective_date
                FROM {table} t
                JOIN document_versions dv ON t.document_version_id = dv.document_version_id
                WHERE t.state_code = %s
                AND dv.valid_to IS NULL
                ORDER BY t.created_at
            """, (state_code,))
            
            results = cur.fetchall()
            
            if not results:
                print(f"❌ No data found for {state_code} {doc_type}")
                return False
            
            # Get column names
            columns = [desc[0] for desc in cur.description]
            
            # Write to CSV
            import csv
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(columns)
                writer.writerows(results)
            
        conn.close()
        print(f"✅ Exported {len(results)} records to {output_file}")
        return True
        
    except Exception as e:
        print(f"❌ Export failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="SST Management Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python manage.py load data/TX_LOD_v2024.1.csv     # Load single file
  python manage.py bulk /data/sst/tm/               # Bulk load directory
  python manage.py status                           # Show system status
  python manage.py cleanup                          # Clean old logs
  python manage.py export TX LOD                    # Export Texas LOD data
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Load command
    load_parser = subparsers.add_parser('load', help='Load single file')
    load_parser.add_argument('file_path', help='Path to file to load')
    
    # Bulk load command
    bulk_parser = subparsers.add_parser('bulk', help='Bulk load directory')
    bulk_parser.add_argument('directory', help='Directory to bulk load')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show system status')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Cleanup old data')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data to CSV')
    export_parser.add_argument('state_code', help='State code (e.g., TX, CA)')
    export_parser.add_argument('doc_type', choices=['LOD', 'CERT', 'TAP'], help='Document type')
    export_parser.add_argument('--output', help='Output file path')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        if args.command == 'load':
            success = load_single_file(args.file_path)
            sys.exit(0 if success else 1)
        
        elif args.command == 'bulk':
            success = bulk_load_directory(args.directory)
            sys.exit(0 if success else 1)
        
        elif args.command == 'status':
            show_system_status()
        
        elif args.command == 'cleanup':
            cleanup_old_data()
        
        elif args.command == 'export':
            success = export_data(args.state_code, args.doc_type, args.output)
            sys.exit(0 if success else 1)
    
    except KeyboardInterrupt:
        print("\n🛑 Operation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()