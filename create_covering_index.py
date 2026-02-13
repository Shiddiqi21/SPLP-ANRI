import os
import sys
from sqlalchemy import create_engine, text

# Adjust path to find app module
sys.path.append(os.getcwd())
try:
    from app.api.stats_routes import get_db_context
except ImportError:
    # Fallback if import fails (e.g. if script run from different dir)
    from app.database import SessionLocal
    class get_db_context:
         def __enter__(self):
             self.db = SessionLocal()
             return self.db
         def __exit__(self, exc_type, exc_val, exc_tb):
             self.db.close()

def create_covering_index():
    print("Creating Covering Index for DataArsip...")
    
    # Columns to include in the index (Date + Summable Columns)
    # This avoids "Table Lookup" for SUM operations.
    columns = [
        "tanggal",
        "naskah_masuk",
        "naskah_keluar", 
        "disposisi", 
        "berkas",
        "retensi_permanen",
        "retensi_musnah",
        "naskah_ditindaklanjuti"
    ]
    
    index_name = "ix_covering_dashboard_perf"
    
    with get_db_context() as db:
        try:
            # Check existing
            result = db.execute(text(f"SHOW INDEX FROM data_arsip WHERE Key_name = '{index_name}'"))
            if result.fetchone():
                print(f"Index '{index_name}' already exists.")
                return

            print(f"Creating index '{index_name}' on columns: {', '.join(columns)}...")
            # Create Index
            col_str = ", ".join(columns)
            db.execute(text(f"CREATE INDEX {index_name} ON data_arsip({col_str})"))
            db.commit()
            print("SUCCESS: Covering Index created! Dashboard should be instant now.")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    create_covering_index()
