
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database import get_db_context
from app.services.generic_summary_service import GenericSummaryService
from app.services.cache_service import cache

def force_refresh():
    print("--- FORCE REFRESH SUMMARY TABLE ---")
    
    with get_db_context() as db:
        service = GenericSummaryService(db)
        table_id = 1 # data_arsip
        
        print(f"Refreshing Summary for Table ID {table_id}...")
        try:
            # Check if summary exists first
            exists = service.check_summary_exists(table_id)
            print(f"Summary Exists: {exists}")
            
            if exists:
                # Refresh (Recreate)
                print("Summary exists, dropping and recreating...")
                result = service.create_summary_table(table_id)
                print(f"Recreate Result: {result}")
            else:
                # Create if not exists
                print("Summary not found, attempting create...")
                result = service.create_summary_table(table_id)
                print(f"Create Result: {result}")
                
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

    # Invalidate Cache
    print("Invalidating Cache...")
    cache.invalidate_prefix("stats_table")
    cache.delete("dashboard_stats")
    cache.invalidate_prefix("grafana")
    
    print("Done!")

if __name__ == "__main__":
    force_refresh()
