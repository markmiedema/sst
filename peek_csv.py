#!/usr/bin/env python3
"""
peek_csv.py - Peek at the structure of SST CSV files
"""

from pathlib import Path
import csv
from config import config

def peek_csv_file(file_path: Path, rows: int = 5):
    """Show the structure and first few rows of a CSV file."""
    print(f"📄 File: {file_path.name}")
    print(f"📍 Path: {file_path}")
    print("=" * 80)
    
    try:
        # Read with different encodings
        for encoding in ['utf-8-sig', 'utf-8', 'latin-1']:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    # Show first few lines raw
                    print(f"\n🔍 First 3 lines (raw) - Encoding: {encoding}:")
                    print("-" * 40)
                    f.seek(0)
                    for i in range(3):
                        line = f.readline()
                        if line:
                            print(f"Line {i+1}: {repr(line[:100])}...")
                    
                    # Parse as CSV
                    f.seek(0)
                    reader = csv.DictReader(f)
                    
                    # Show headers
                    print(f"\n📊 CSV Headers:")
                    print("-" * 40)
                    if reader.fieldnames:
                        for i, field in enumerate(reader.fieldnames, 1):
                            print(f"{i:2d}. '{field}'")
                    
                    # Show sample rows
                    print(f"\n📋 First {rows} rows:")
                    print("-" * 40)
                    for i, row in enumerate(reader):
                        if i >= rows:
                            break
                        print(f"\nRow {i+1}:")
                        for key, value in row.items():
                            if value:  # Only show non-empty values
                                print(f"  {key}: {value[:50]}{'...' if len(str(value)) > 50 else ''}")
                    
                    print(f"\n✅ Successfully read with {encoding} encoding")
                    break
                    
            except UnicodeDecodeError:
                print(f"❌ Failed with {encoding} encoding")
                continue
                
    except Exception as e:
        print(f"❌ Error reading file: {e}")

# Peek at one file of each type
if __name__ == "__main__":
    data_lake = config.loading.data_lake_path
    
    test_files = [
        data_lake / "tm" / "state=AR" / "tm_AR_v2024.0_20250706T220321.csv",
        data_lake / "tap" / "state=AR" / "tap_AR_v2024.1_20250706T212810.csv",
        data_lake / "cc" / "state=AR" / "cc_AR_v2024.0_20250706T224458.csv"
    ]
    
    for file_path in test_files:
        if file_path.exists():
            peek_csv_file(file_path, rows=3)
            print("\n" + "="*80 + "\n")
        else:
            print(f"❌ File not found: {file_path}")