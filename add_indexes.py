from sqlalchemy import text
from app.database import get_db_context
from app.models.table_models import TableDefinition

def add_indexes():
    print("Starting index creation...")
    with get_db_context() as db:
        tables = db.query(TableDefinition).all()
        print(f"Found {len(tables)} tables.")
        
        for table in tables:
            safe_name = table.name # Assuming it's already sanitized/safe from creation
            print(f"Processing table: {table.display_name} ({safe_name})...")
            
            # Index for unit_kerja_id
            idx_unit_name = f"idx_{safe_name}_unit_kerja_id"
            sql_unit = f"CREATE INDEX IF NOT EXISTS {idx_unit_name} ON {safe_name} (unit_kerja_id);"
            
            # Index for tanggal
            idx_tanggal_name = f"idx_{safe_name}_tanggal"
            sql_tanggal = f"CREATE INDEX IF NOT EXISTS {idx_tanggal_name} ON {safe_name} (tanggal);"
            
            # Composite Index for common filtering (unit + tanggal) - Optional but good
            idx_composite = f"idx_{safe_name}_unit_tanggal"
            sql_composite = f"CREATE INDEX IF NOT EXISTS {idx_composite} ON {safe_name} (unit_kerja_id, tanggal);"

            try:
                print(f"  - Adding index {idx_unit_name}...")
                db.execute(text(sql_unit))
                
                print(f"  - Adding index {idx_tanggal_name}...")
                db.execute(text(sql_tanggal))
                
                print(f"  - Adding composite index {idx_composite}...")
                db.execute(text(sql_composite))
                
                db.commit()
                print("  Success.")
            except Exception as e:
                db.rollback()
                print(f"  Error adding index for {safe_name}: {e}")

    print("Index creation completed.")

if __name__ == "__main__":
    add_indexes()
