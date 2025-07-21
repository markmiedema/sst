#!/usr/bin/env python3
"""
monitor.py - Quick command-line monitoring tool for SST loading operations.

Usage:
    python monitor.py status                    # Show loading status summary
    python monitor.py failed                   # Show failed loads
    python monitor.py performance               # Show performance metrics
    python monitor.py retry                    # Retry failed loads
    python monitor.py validate [state_code]    # Run validation checks
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config import get_connection
from monitoring.dashboard import LoadingMonitor, LoadingRecovery
from loader.validation import ValidationOrchestrator
from loader.sst_loader import SSTDatabaseLoader


def format_summary(summary_data):
    """Format loading summary for console output."""
    print(f"\n📊 Loading Summary - {summary_data['period']}")
    print("=" * 50)
    
    if not summary_data['summary']:
        print("No loading activity found.")
        return
    
    for status, data in summary_data['summary'].items():
        status_emoji = {
            'completed': '✅',
            'failed': '❌',
            'in_progress': '🔄'
        }.get(status, '📋')
        
        print(f"{status_emoji} {status.upper()}: {data['count']} loads")
        print(f"   States affected: {data['states']}")
        print(f"   Document types: {data['doc_types']}")
        print()


def format_failed_loads(failed_loads):
    """Format failed loads for console output."""
    print(f"\n❌ Failed Loads ({len(failed_loads)} found)")
    print("=" * 50)
    
    if not failed_loads:
        print("No failed loads found. 🎉")
        return
    
    for load in failed_loads:
        print(f"🏛️  {load['state_code']} - {load['document_type']} v{load['version']}")
        print(f"   Attempts: {load['attempt_count']}")
        print(f"   Last attempt: {load['last_attempt']}")
        print(f"   Error: {load['error_message'][:100]}{'...' if len(load['error_message']) > 100 else ''}")
        print()


def format_performance(perf_data):
    """Format performance metrics for console output."""
    print("\n⚡ Performance Metrics")
    print("=" * 50)
    
    if not perf_data.get('by_document_type'):
        print("No performance data available.")
        return
    
    for doc_type, metrics in perf_data['by_document_type'].items():
        print(f"📄 {doc_type}:")
        print(f"   Average: {metrics['avg_seconds']:.1f}s")
        print(f"   Range: {metrics['min_seconds']:.1f}s - {metrics['max_seconds']:.1f}s")
        print(f"   Sample size: {metrics['sample_size']} loads")
        print()


def format_validation_report(report):
    """Format validation report for console output."""
    print(f"\n🔍 Validation Report")
    print("=" * 50)
    print(f"Generated: {report['timestamp']}")
    print(f"Scope: {'All states' if not report['state_filter'] else report['state_filter']}")
    print(f"Summary: {report['summary']['errors']} errors, {report['summary']['warnings']} warnings")
    print()
    
    if report['summary']['errors'] == 0 and report['summary']['warnings'] == 0:
        print("✅ All validations passed!")
        return
    
    # Show state-specific results
    for state, results in report['results'].items():
        if state == 'data_freshness':
            continue
            
        has_issues = any(
            not result.get('valid', True) or result.get('errors') or result.get('warnings')
            for result in results.values()
        )
        
        if has_issues:
            print(f"🏛️  {state}:")
            for check_name, result in results.items():
                if result.get('errors'):
                    for error in result['errors']:
                        print(f"   ❌ {check_name}: {error}")
                if result.get('warnings'):
                    for warning in result['warnings']:
                        print(f"   ⚠️  {check_name}: {warning}")
            print()
    
    # Show data freshness warnings
    if 'data_freshness' in report['results']:
        freshness = report['results']['data_freshness']
        if freshness.get('warnings'):
            print("🕒 Data Freshness Warnings:")
            for warning in freshness['warnings']:
                print(f"   ⚠️  {warning}")


def main():
    parser = argparse.ArgumentParser(
        description="SST Loading Monitor - Quick status and recovery tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python monitor.py status --days 3        # Last 3 days summary
  python monitor.py failed                 # Show retry-eligible failures
  python monitor.py performance            # Performance metrics
  python monitor.py retry                  # Attempt to retry failed loads
  python monitor.py validate TX            # Validate Texas data
  python monitor.py validate               # Validate all states
        """
    )
    
    parser.add_argument(
        'command',
        choices=['status', 'failed', 'performance', 'retry', 'validate'],
        help='Command to execute'
    )
    parser.add_argument(
        'state_code',
        nargs='?',
        help='State code for validation (optional)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Days to look back for status/failed commands (default: 7)'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output in JSON format instead of formatted text'
    )
    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='Maximum retry attempts for failed loads (default: 3)'
    )
    
    args = parser.parse_args()
    
    try:
        conn = get_connection()
        monitor = LoadingMonitor(conn)
        
        if args.command == 'status':
            result = monitor.get_loading_summary(args.days)
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                format_summary(result)
        
        elif args.command == 'failed':
            result = monitor.get_failed_loads(retry_eligible=True)
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                format_failed_loads(result)
        
        elif args.command == 'performance':
            result = monitor.get_loading_performance()
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                format_performance(result)
        
        elif args.command == 'retry':
            loader = SSTDatabaseLoader(conn)
            recovery = LoadingRecovery(conn, loader)
            result = recovery.retry_failed_loads(max_retries=args.max_retries)
            
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                print(f"\n🔄 Retry Results")
                print("=" * 30)
                print(f"Attempted: {result['attempted']}")
                print(f"Succeeded: {result['succeeded']} ✅")
                print(f"Failed: {result['failed']} ❌")
                
                if result['details']:
                    print("\nDetails:")
                    for detail in result['details']:
                        load = detail['load']
                        status = detail.get('status', 'failed')
                        error = detail.get('error', '')
                        
                        status_emoji = '✅' if status == 'success' else '❌'
                        print(f"  {status_emoji} {load['state_code']} {load['document_type']} {load['version']}")
                        if error:
                            print(f"     Error: {error}")
        
        elif args.command == 'validate':
            validator = ValidationOrchestrator(conn)
            result = validator.validate_all(state_code=args.state_code)
            
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                format_validation_report(result)
        
        conn.close()
        
    except KeyboardInterrupt:
        print("\n🛑 Operation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()