from app.database import get_db_context
from sqlalchemy import text

def clean_units():
    with get_db_context() as db:
        # Find units with "(Lama)"
        sql = "SELECT id, nama FROM unit_kerja WHERE nama LIKE '%(Lama)%'"
        targets = db.execute(text(sql)).mappings().all()
        
        print(f"Found {len(targets)} units to delete:")
        for t in targets:
            print(f" - {t['nama']} (ID {t['id']})")
            
            # Check if used
            usage = db.execute(text(f"SELECT COUNT(*) FROM data_arsip WHERE unit_kerja_id = {t['id']}")).scalar()
            print(f"   Usage: {usage} records")
            
            if usage == 0:
                print("   Deleting...")
                try:
                    db.execute(text(f"DELETE FROM unit_kerja WHERE id = {t['id']}"))
                    db.commit()
                    print("   Deleted.")
                except Exception as e:
                    print(f"   Error: {e}")
            else:
                print("   SKIPPED (Has Data)")

if __name__ == "__main__":
    clean_units()
