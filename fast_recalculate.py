
import os
import sys
import time

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text
from app.database import get_db_context
from app.services.cache_service import cache

def fast_recalculate():
    print("--- FAST RECALCULATE DATA ARSIP TOTALS (SQL) ---")
    start_time = time.time()
    
    sql = """
    UPDATE data_arsip
    SET total = 
        COALESCE(naskah_masuk, 0) + 
        COALESCE(naskah_keluar, 0) + 
        COALESCE(disposisi, 0) + 
        COALESCE(berkas, 0) + 
        COALESCE(retensi_permanen, 0) + 
        COALESCE(retensi_musnah, 0) + 
        COALESCE(naskah_ditindaklanjuti, 0)
    """
    
    with get_db_context() as db:
        print("Executing SQL Update...")
        result = db.execute(text(sql))
        db.commit()
        print(f"Update complete. Rows affected: {result.rowcount}")
        
    elapsed = time.time() - start_time
    print(f"Time taken: {elapsed:.2f} seconds")

    # Invalidate Cache
    print("Invalidating Cache...")
    cache.invalidate_prefix("stats_table")
    cache.delete("dashboard_stats")
    cache.invalidate_prefix("grafana")
    print("Done!")

if __name__ == "__main__":
    fast_recalculate()
