from app.database import get_db_context
from sqlalchemy import text

def delete_newest():
    with get_db_context() as db:
        # Find 6 newest units
        # User said "upload terakhir kali" -> Highest IDs
        sql = "SELECT id, nama FROM unit_kerja ORDER BY id DESC LIMIT 6"
        targets = db.execute(text(sql)).mappings().all()
        
        print(f"Found {len(targets)} newest units to delete:")
        ids = []
        for t in targets:
            print(f" - {t['id']}: {t['nama']}")
            ids.append(t['id'])
            
        if not ids:
            print("No units found.")
            return

        # Check data impact
        total_data = 0
        for i in ids:
            c = db.execute(text(f"SELECT COUNT(*) FROM data_arsip WHERE unit_kerja_id = {i}")).scalar()
            total_data += c
            
        print(f"\nTotal data records to be deleted: {total_data}")
        
        # confirm deletion (auto-proceed based on user request)
        print("Deleting...")
        try:
             db.execute(text(f"DELETE FROM unit_kerja WHERE id IN ({','.join(map(str, ids))})"))
             db.commit()
             # Verify count
             count = db.execute(text("SELECT COUNT(*) FROM unit_kerja")).scalar()
             print(f"Deletion successful. Remaining Units: {count}")
        except Exception as e:
             print(f"Error: {e}")
             db.rollback()

if __name__ == "__main__":
    delete_newest()
