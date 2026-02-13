import os
import sys
from sqlalchemy import create_engine, text

# Adjust path to find app module
sys.path.append(os.getcwd())
try:
    from app.api.stats_routes import get_db_context
except ImportError:
    from app.database import SessionLocal
    class get_db_context:
         def __enter__(self):
             self.db = SessionLocal()
             return self.db
         def __exit__(self, exc_type, exc_val, exc_tb):
             self.db.close()

def fix_covering_index():
    print("Upgrading Covering Index for DataArsip...")
    
    # OLD strict index (missing unit_kerja_id)
    old_index_name = "ix_covering_dashboard_perf"
    # NEW covering index
    new_index_name = "ix_covering_dashboard_perf_v2"
    
    # Columns including unit_kerja_id for JOIN optimization
    columns = [
        "tanggal",
        "unit_kerja_id", # Added!
        "naskah_masuk",
        "naskah_keluar", 
        "disposisi", 
        "berkas",
        "retensi_permanen",
        "retensi_musnah",
        "naskah_ditindaklanjuti"
    ]
    
    with get_db_context() as db:
        try:
            # 1. Drop old index if exists
            print(f"Dropping old index '{old_index_name}'...")
            try:
                db.execute(text(f"DROP INDEX {old_index_name} ON data_arsip"))
                print("Old index dropped.")
            except Exception as e:
                print(f"Old index not found or error: {e}")

            # 2. Check/Create new index
            print(f"Checking new index '{new_index_name}'...")
            result = db.execute(text(f"SHOW INDEX FROM data_arsip WHERE Key_name = '{new_index_name}'"))
            if result.fetchone():
                print(f"Index '{new_index_name}' already exists.")
                return

            print(f"Creating index '{new_index_name}' on columns: {', '.join(columns)}...")
            col_str = ", ".join(columns)
            db.execute(text(f"CREATE INDEX {new_index_name} ON data_arsip({col_str})"))
            db.commit()
            print("SUCCESS: Upgraded Covering Index created!")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    fix_covering_index()
