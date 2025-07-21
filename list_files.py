#!/usr/bin/env python3
"""
list_files.py - List available SST files for testing
"""

from pathlib import Path
from config import config

def list_sst_files(limit=10):
    """List available SST files organized by type."""
    data_lake = config.loading.data_lake_path
    
    print(f"📁 SST Files in: {data_lake}")
    print("=" * 70)
    
    doc_types = {
        "tm": "LOD (Library of Definitions)",
        "tap": "TAP (Tax Administration Practices)", 
        "cc": "CERT (Certificate of Compliance)"
    }
    
    for folder, description in doc_types.items():
        folder_path = data_lake / folder
        if not folder_path.exists():
            print(f"\n❌ {description} folder not found: {folder_path}")
            continue
            
        print(f"\n📄 {description} Files:")
        print("-" * 50)
        
        count = 0
        for state_dir in sorted(folder_path.glob("state=*")):
            state_code = state_dir.name.split("=")[1]
            
            for csv_file in sorted(state_dir.glob("*.csv")):
                count += 1
                print(f"{count:3d}. {state_code} - {csv_file.name}")
                
                # Print the full command to test this file
                if count == 1:  # Show example command for first file
                    print(f"     Test command: python main.py {folder} {state_code} \"{csv_file}\"")
                
                if count >= limit:
                    remaining = len(list(folder_path.rglob("*.csv"))) - count
                    if remaining > 0:
                        print(f"     ... and {remaining} more files")
                    break
            
            if count >= limit:
                break
    
    print("\n" + "=" * 70)
    print("💡 To test a specific file, use:")
    print('   python main.py <type> <state> "<full_path>"')
    print("   Example: python main.py tm AR \"D:\\DataLake\\raw\\sst\\tm\\state=AR\\tm_AR_v2024.0_20250706T220321.csv\"")

if __name__ == "__main__":
    list_sst_files()