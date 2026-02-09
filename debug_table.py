from app.services.table_service import table_service
from app.database import get_db_context
import json
import traceback

def debug_table_load():
    print("--- Debugging Table Load ---")
    try:
        # Simulate format used by API
        result = table_service.get_dynamic_data(table_id=1, limit=10, offset=0)
        
        print(f"Total Records: {result.get('total')}")
        print(f"Data Length: {len(result.get('data', []))}")
        
        if result.get('data'):
            print("First Record Sample:")
            print(result['data'][0])
        else:
            print("No data returned.")
            
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    debug_table_load()
