from app.database import get_db_context
from sqlalchemy import text

def check_progress():
    with get_db_context() as db:
        # Check counts of the 6 units targetted (IDs 78-83)
        # If they are gone, count is 0.
        # If cascading, data count for these units should be decreasing.
        
        ids = [78, 79, 80, 81, 82, 83]
        params = ",".join(map(str, ids))
        
        # Check if units still exist
        units = db.execute(text(f"SELECT id FROM unit_kerja WHERE id IN ({params})")).mappings().all()
        print(f"Target Units Remaining: {len(units)} / 6")
        
        # Check Remaining Data for these units
        data_count = db.execute(text(f"SELECT COUNT(*) FROM data_arsip WHERE unit_kerja_id IN ({params})")).scalar()
        print(f"Remaining Data Records to delete: {data_count}")
        
        # Check Total Units
        total_units = db.execute(text("SELECT COUNT(*) FROM unit_kerja")).scalar()
        print(f"Total Unique Units in DB: {total_units}")

if __name__ == "__main__":
    check_progress()
