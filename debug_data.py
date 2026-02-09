from app.services.table_service import table_service
from app.database import get_db_context
from sqlalchemy import text

def debug_system():
    # 1. Check Statistics Output
    print("--- Stats Check ---")
    try:
        stats = table_service.get_statistics(1)
        print(f"Stats Result: {stats}")
    except Exception as e:
        print(f"Stats Error: {e}")

    # 2. Check Duplicates
    print("\n--- Duplicate Check ---")
    with get_db_context() as db:
        sql = """
            SELECT nama, COUNT(*) as cnt, GROUP_CONCAT(id) as ids 
            FROM unit_kerja 
            GROUP BY nama 
            HAVING cnt > 1
        """
        dupes = db.execute(text(sql)).mappings().all()
        if not dupes:
            print("No duplicates found.")
        else:
            print(f"Found {len(dupes)} duplicate groups:")
            for d in dupes:
                print(f" - {d['nama']}: Count={d['cnt']}, IDs=[{d['ids']}]")

if __name__ == "__main__":
    debug_system()
