from sqlalchemy import create_engine, text
from app.config import get_settings

settings = get_settings()
DATABASE_URL = settings.database_url

engine = create_engine(DATABASE_URL)
conn = engine.connect()

def optimize_indexes():
    print("Optimizing Database Indexes...")
    try:
        # Check existing indexes first
        existing = conn.execute(text("SHOW INDEX FROM data_arsip")).fetchall()
        index_names = [row[2] for row in existing]
        print(f"Existing Indexes: {index_names}")
        
        # 1. Add Index on unit_kerja_id if missing
        if "ix_data_arsip_unit_kerja_id" not in index_names and "fk_data_arsip_unit_kerja_id" not in index_names:
             # Check if any index covers unit_kerja_id as first column
             covered = any(row[4] == 'unit_kerja_id' and row[3] == 1 for row in existing)
             if not covered:
                 print("Adding Index on unit_kerja_id...")
                 conn.execute(text("CREATE INDEX ix_data_arsip_unit_kerja_id ON data_arsip (unit_kerja_id)"))
                 print("Done.")
             else:
                 print("Index on unit_kerja_id already exists (covered).")

        # 2. Add Composite Index (unit_kerja_id, tanggal) for Range Queries
        # This is the "killer feature" for dashboard performance
        if "ix_composite_unit_tanggal" not in index_names:
            print("Adding Composite Index (unit_kerja_id, tanggal)...")
            try:
                conn.execute(text("CREATE INDEX ix_composite_unit_tanggal ON data_arsip (unit_kerja_id, tanggal)"))
                print("Done.")
            except Exception as e:
                 print(f"Error creating composite: {e}")
        else:
            print("Composite Index already exists.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    optimize_indexes()
