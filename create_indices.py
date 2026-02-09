from sqlalchemy import text
from app.database import get_db_context

def create_indices():
    with get_db_context() as db:
        print("Creating indices on data_arsip...")
        columns = [
            "naskah_masuk", 
            "naskah_keluar", 
            "disposisi", 
            "berkas", 
            "retensi_permanen", 
            "retensi_musnah", 
            "naskah_ditindaklanjuti"
        ]
        
        for col in columns:
            try:
                # Check if index exists not needed, create index if not exists logic is implicit or use catch
                # Generic SQL: CREATE INDEX IF NOT EXISTS (supported in postgres/sqlite)
                # MySQL: CREATE INDEX ... (no IF NOT EXISTS in old versions, but usually fine)
                
                idx_name = f"ix_data_arsip_{col}"
                sql = f"CREATE INDEX {idx_name} ON data_arsip ({col})"
                print(f"Creating {idx_name}...")
                db.execute(text(sql))
                db.commit()
            except Exception as e:
                print(f"Skipping {col}: {e}")
                db.rollback()
                
        print("Indices created.")

if __name__ == "__main__":
    create_indices()
