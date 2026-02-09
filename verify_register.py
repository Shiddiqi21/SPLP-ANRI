
import sys
import os
import asyncio
from app.services.table_service import table_service
from app.services.schema_inspector import schema_inspector

# Add project root to path
sys.path.append(os.getcwd())

def register_test():
    table_name = "data_arsip"
    display_name = "Laporan Harian (Fixed)"
    
    print(f"Attempting to register '{table_name}'...")
    
    # 1. Get Columns
    columns = schema_inspector.get_table_columns(table_name)
    print(f"Found {len(columns)} columns: {[c['name'] for c in columns]}")
    
    if not columns:
        print("ERROR: Table not found or no columns.")
        return

    # 2. Register
    result = table_service.register_existing_table(
        name=table_name,
        display_name=display_name,
        description="Registered via Verification Script",
        columns=columns
    )
    
    print("Result:", result)
    
    if result.get("status") == "success":
        print("✅ REGISTRATION SUCCESSFUL")
    else:
        print("❌ REGISTRATION FAILED")

if __name__ == "__main__":
    register_test()
