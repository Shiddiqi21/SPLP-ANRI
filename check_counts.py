from sqlalchemy import text
from app.database import get_db_context
from app.config import get_settings

def check_data():
    settings = get_settings()
    print(f"DB URL: {settings.database_url}")
    
    with get_db_context() as db:
        try:
            instansi = db.execute(text("SELECT COUNT(*) FROM instansi")).scalar()
            units = db.execute(text("SELECT COUNT(*) FROM unit_kerja")).scalar()
            data = db.execute(text("SELECT COUNT(*) FROM data_arsip")).scalar()
            
            print(f"Instansi: {instansi}")
            print(f"Unit Kerja: {units}")
            print(f"Data Arsip: {data}")
            
            if data > 0:
                # Check sample
                sample = db.execute(text("SELECT unit_kerja_id FROM data_arsip LIMIT 1")).scalar()
                print(f"Sample Data UnitID: {sample}")
                
                # Check if this UnitID exists
                if sample:
                    u_exists = db.execute(text(f"SELECT COUNT(*) FROM unit_kerja WHERE id={sample}")).scalar()
                    print(f"Ref Unit Exists: {u_exists}")

        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    check_data()
