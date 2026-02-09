from app.database import get_db_context
from sqlalchemy import text

def list_units():
    with get_db_context() as db:
        units = db.execute(text("SELECT id, nama FROM unit_kerja ORDER BY nama")).mappings().all()
        print(f"Total Units: {len(units)}")
        for u in units:
            print(f"{u['id']}: {u['nama']}")

if __name__ == "__main__":
    list_units()
