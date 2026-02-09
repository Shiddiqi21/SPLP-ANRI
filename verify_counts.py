import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from app.database import SessionLocal
from app.models.arsip_models import Instansi, UnitKerja, DataArsip
from sqlalchemy import func

def check_counts():
    db = SessionLocal()
    try:
        count_instansi = db.query(func.count(Instansi.id)).scalar()
        count_unit = db.query(func.count(UnitKerja.id)).scalar()
        count_data = db.query(func.count(DataArsip.id)).scalar()
        
        print(f"Total Instansi: {count_instansi}")
        print(f"Total Unit Kerja: {count_unit}")
        print(f"Total Data Arsip: {count_data}")
        
        # List all Instansi to see what's there
        print("\nDaftar Instansi:")
        instansis = db.query(Instansi).all()
        for i in instansis:
            print(f"- [{i.id}] {i.kode} - {i.nama}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    check_counts()
