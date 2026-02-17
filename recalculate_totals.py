
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database import get_db_context
from app.models.arsip_models import DataArsip
from app.services.cache_service import cache

def recalculate_totals():
    print("--- RECALCULATING DATA ARSIP TOTALS ---")
    
    with get_db_context() as db:
        # Fetch all records
        records = db.query(DataArsip).all()
        count = len(records)
        print(f"Found {count} records.")
        
        batch_size = 1000
        processed = 0
        
        for record in records:
            old_total = record.total
            new_total = record.calculate_total()
            
            # Optional: Print changes for first few to verify
            if processed < 5:
                print(f"ID {record.id}: Old Total={old_total} -> New Total={new_total} (Diff: {new_total - old_total})")
            
            processed += 1
            if processed % batch_size == 0:
                db.commit()
                print(f"Processed {processed}/{count}...")
        
        db.commit()
        print("Recalculation complete.")
        
        # Invalidate Cache
        print("Invalidating Cache...")
        cache.invalidate_prefix("stats_table")
        cache.delete("dashboard_stats")
        # Also clear grafana cache
        cache.invalidate_prefix("grafana")
        
        print("Done!")

if __name__ == "__main__":
    recalculate_totals()
