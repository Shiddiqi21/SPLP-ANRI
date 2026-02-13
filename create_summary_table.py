import os
import sys
from sqlalchemy import create_engine, text
import time

# Adjust path
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

def create_summary_table():
    print("Optimization Phase 3: Creating Summary Table (Materialized View)...")
    
    table_name = "data_arsip_monthly_summary"
    
    sql_create = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        year INT NOT NULL,
        month INT NOT NULL,
        unit_kerja_id INT NOT NULL,
        naskah_masuk INT DEFAULT 0,
        naskah_keluar INT DEFAULT 0,
        disposisi INT DEFAULT 0,
        berkas INT DEFAULT 0,
        retensi_permanen INT DEFAULT 0,
        retensi_musnah INT DEFAULT 0,
        naskah_ditindaklanjuti INT DEFAULT 0,
        total INT DEFAULT 0,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_summary_year (year),
        INDEX idx_summary_unit (unit_kerja_id)
    ) ENGINE=InnoDB;
    """
    
    sql_populate = f"""
    INSERT INTO {table_name} 
    (year, month, unit_kerja_id, naskah_masuk, naskah_keluar, disposisi, berkas, retensi_permanen, retensi_musnah, naskah_ditindaklanjuti, total)
    SELECT 
        YEAR(tanggal) as year,
        MONTH(tanggal) as month,
        unit_kerja_id,
        SUM(naskah_masuk),
        SUM(naskah_keluar),
        SUM(disposisi),
        SUM(berkas),
        SUM(retensi_permanen),
        SUM(retensi_musnah),
        SUM(naskah_ditindaklanjuti),
        SUM(total)
    FROM data_arsip
    GROUP BY YEAR(tanggal), MONTH(tanggal), unit_kerja_id
    """
    
    with get_db_context() as db:
        try:
            print(f"1. Resetting table '{table_name}'...")
            db.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            db.execute(text(sql_create))
            
            print("2. Populating Summary Data (Aggregating 2M rows)...")
            start_t = time.time()
            db.execute(text(sql_populate))
            db.commit()
            duration = time.time() - start_t
            
            print(f"SUCCESS! Summary Table Created in {duration:.2f}s.")
            
            # Verify count
            count = db.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            print(f"Summary Table Rows: {count} (Reflecting full dataset)")
            
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    create_summary_table()
