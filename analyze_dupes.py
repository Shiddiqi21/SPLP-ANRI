from app.database import get_db_context
from sqlalchemy import text

def analyze_dupes():
    with get_db_context() as db:
        # Check case-insensitive duplicates
        sql = """
            SELECT LOWER(TRIM(nama)) as norm_nama, COUNT(*) as cnt, GROUP_CONCAT(id) as ids
            FROM unit_kerja 
            GROUP BY norm_nama 
            HAVING cnt > 1
        """
        dupes = db.execute(text(sql)).mappings().all()
        
        print(f"Found {len(dupes)} duplicate groups (Fuzzy):")
        for d in dupes:
            print(f" - '{d['norm_nama']}': Count={d['cnt']}, IDs=[{d['ids']}]")

if __name__ == "__main__":
    analyze_dupes()
