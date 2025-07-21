# monitoring/dashboard.py
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json
from pathlib import Path

class LoadingMonitor:
    """Monitor and report on loading status."""
    
    def __init__(self, conn):
        self.conn = conn
        
    def get_loading_summary(self, days_back: int = 7) -> Dict:
        """Get summary of recent loading activity."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    status,
                    COUNT(*) as count,
                    COUNT(DISTINCT state_code) as states_affected,
                    COUNT(DISTINCT document_type) as doc_types
                FROM loading_status
                WHERE started_at >= %s
                GROUP BY status
            """, (datetime.now() - timedelta(days=days_back),))
            
            status_summary = {
                row[0]: {
                    'count': row[1],
                    'states': row[2],
                    'doc_types': row[3]
                }
                for row in cur.fetchall()
            }
            
        return {
            'period': f'Last {days_back} days',
            'summary': status_summary,
            'generated_at': datetime.now().isoformat()
        }
    
    def get_failed_loads(self, retry_eligible: bool = True) -> List[Dict]:
        """Get failed loads that need attention."""
        with self.conn.cursor() as cur:
            query = """
                SELECT DISTINCT ON (state_code, document_type, version)
                    id, state_code, document_type, version, 
                    file_hash, error_message, started_at,
                    COUNT(*) OVER (PARTITION BY state_code, document_type, version) as attempt_count
                FROM loading_status
                WHERE status = 'failed'
                ORDER BY state_code, document_type, version, started_at DESC
            """
            
            if retry_eligible:
                query = """
                    WITH failed_attempts AS (
                        SELECT state_code, document_type, version,
                               COUNT(*) as attempts
                        FROM loading_status
                        WHERE status = 'failed'
                        GROUP BY state_code, document_type, version
                    )
                    SELECT ls.id, ls.state_code, ls.document_type, 
                           ls.version, ls.file_hash, ls.error_message, 
                           ls.started_at, fa.attempts
                    FROM loading_status ls
                    JOIN failed_attempts fa USING (state_code, document_type, version)
                    WHERE ls.status = 'failed'
                    AND fa.attempts < 3  -- Max retry attempts
                    AND ls.started_at = (
                        SELECT MAX(started_at)
                        FROM loading_status ls2
                        WHERE ls2.state_code = ls.state_code
                        AND ls2.document_type = ls.document_type
                        AND ls2.version = ls.version
                    )
                """
            
            cur.execute(query)
            
            return [
                {
                    'id': row[0],
                    'state_code': row[1],
                    'document_type': row[2],
                    'version': row[3],
                    'file_hash': row[4],
                    'error_message': row[5],
                    'last_attempt': row[6].isoformat(),
                    'attempt_count': row[7]
                }
                for row in cur.fetchall()
            ]
    
    def get_loading_performance(self) -> Dict:
        """Analyze loading performance metrics."""
        with self.conn.cursor() as cur:
            # Average load time by document type
            cur.execute("""
                SELECT 
                    document_type,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_seconds,
                    MIN(EXTRACT(EPOCH FROM (completed_at - started_at))) as min_seconds,
                    MAX(EXTRACT(EPOCH FROM (completed_at - started_at))) as max_seconds,
                    COUNT(*) as sample_size
                FROM loading_status
                WHERE status = 'completed'
                AND completed_at IS NOT NULL
                GROUP BY document_type
            """)
            
            performance_by_type = {
                row[0]: {
                    'avg_seconds': round(row[1], 2),
                    'min_seconds': round(row[2], 2),
                    'max_seconds': round(row[3], 2),
                    'sample_size': row[4]
                }
                for row in cur.fetchall()
            }
            
        return {
            'by_document_type': performance_by_type,
            'generated_at': datetime.now().isoformat()
        }


class LoadingRecovery:
    """Handle recovery of failed loads."""
    
    def __init__(self, conn, loader):
        self.conn = conn
        self.loader = loader
        self.monitor = LoadingMonitor(conn)
        
    def retry_failed_loads(self, max_retries: int = 3) -> Dict:
        """Retry failed loads with exponential backoff."""
        failed_loads = self.monitor.get_failed_loads(retry_eligible=True)
        results = {
            'attempted': 0,
            'succeeded': 0,
            'failed': 0,
            'details': []
        }
        
        for load in failed_loads:
            if load['attempt_count'] >= max_retries:
                continue
                
            results['attempted'] += 1
            
            # Exponential backoff based on attempt count
            wait_seconds = 2 ** load['attempt_count']
            
            try:
                # Find the file based on the stored pattern
                file_path = self._find_file_for_retry(load)
                if not file_path:
                    results['failed'] += 1
                    results['details'].append({
                        'load': load,
                        'error': 'File not found for retry'
                    })
                    continue
                
                # Retry the load
                self.loader.load_combined(
                    file_path,
                    load['document_type'],
                    load['state_code'],
                    self._get_state_name(load['state_code']),
                    load['version']
                )
                
                results['succeeded'] += 1
                results['details'].append({
                    'load': load,
                    'status': 'success'
                })
                
            except Exception as e:
                results['failed'] += 1
                results['details'].append({
                    'load': load,
                    'error': str(e)
                })
                
                # Log the retry failure
                from loader.change_detector import mark_status
                mark_status(
                    self.conn,
                    load['state_code'],
                    load['document_type'],
                    load['version'],
                    load['file_hash'],
                    'failed',
                    error=f'Retry {load["attempt_count"] + 1}: {str(e)}'
                )
        
        return results
    
    def _find_file_for_retry(self, load: Dict) -> Optional[Path]:
        """Find the file to retry based on load metadata."""
        # Map document types to folder names
        folder_map = {'LOD': 'tm', 'TAP': 'tap', 'CERT': 'cc'}
        folder = folder_map.get(load['document_type'])
        
        if not folder:
            return None
        
        # Construct expected path
        base_path = Path(r"D:\DataLake\raw\sst") / folder / f"state={load['state_code']}"
        
        # Look for files matching the version
        for file in base_path.glob(f"*_{load['version']}_*.csv"):
            # Verify file hash if needed
            from loader.change_detector import sha256
            if sha256(file) == load['file_hash']:
                return file
        
        return None
    
    def _get_state_name(self, state_code: str) -> str:
        """Get state name from database or config."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT state_name FROM states WHERE state_code = %s",
                (state_code,)
            )
            result = cur.fetchone()
            if result:
                return result[0]
        
        # Fallback to config file
        import json
        state_names = json.load(open("config/state_names.json"))
        return state_names.get(state_code, state_code)


# CLI tool for monitoring
if __name__ == "__main__":
    import argparse
    from loader.db import get_connection
    
    parser = argparse.ArgumentParser(description="SST Loading Monitor")
    parser.add_argument('command', choices=['summary', 'failed', 'performance', 'retry'])
    parser.add_argument('--days', type=int, default=7, help="Days to look back")
    parser.add_argument('--format', choices=['json', 'text'], default='text')
    
    args = parser.parse_args()
    
    conn = get_connection()
    monitor = LoadingMonitor(conn)
    
    if args.command == 'summary':
        result = monitor.get_loading_summary(args.days)
    elif args.command == 'failed':
        result = monitor.get_failed_loads()
    elif args.command == 'performance':
        result = monitor.get_loading_performance()
    elif args.command == 'retry':
        from loader.sst_loader import SSTDatabaseLoader
        loader = SSTDatabaseLoader(conn)
        recovery = LoadingRecovery(conn, loader)
        result = recovery.retry_failed_loads()
    
    if args.format == 'json':
        print(json.dumps(result, indent=2))
    else:
        # Pretty print for text format
        if args.command == 'summary':
            print(f"\nLoading Summary - {result['period']}")
            print("-" * 40)
            for status, data in result['summary'].items():
                print(f"{status}: {data['count']} loads across "
                      f"{data['states']} states and {data['doc_types']} doc types")
        elif args.command == 'failed':
            print(f"\nFailed Loads ({len(result)} found)")
            print("-" * 40)
            for load in result:
                print(f"{load['state_code']} {load['document_type']} "
                      f"{load['version']} - Attempts: {load['attempt_count']}")
                print(f"  Error: {load['error_message']}")
                print(f"  Last attempt: {load['last_attempt']}\n")
    
    conn.close()