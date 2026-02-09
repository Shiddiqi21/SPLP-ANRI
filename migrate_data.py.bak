
import sys
import os
from datetime import date

# Add parent directory to path so we can import app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import get_db_context
from app.models.arsip_models import DataArsip
from app.models.table_models import TableDefinition, ColumnDefinition, DynamicData
from app.services.table_service import table_service

def migrate_data():
    print("Starting data migration...")
    
    with get_db_context() as db:
        # 1. Create Default Table Definition
        print(" Creating default table definition...")
        result = table_service.create_table(
            name="data_arsip",
            display_name="Data Arsip",
            description="Tabel default untuk data arsip (Migrated)",
            columns=[
                {"name": "naskah_masuk", "display_name": "Naskah Masuk", "data_type": "integer", "is_summable": True},
                {"name": "naskah_keluar", "display_name": "Naskah Keluar", "data_type": "integer", "is_summable": True},
                {"name": "disposisi", "display_name": "Disposisi", "data_type": "integer", "is_summable": True},
                {"name": "berkas", "display_name": "Berkas", "data_type": "integer", "is_summable": True},
                {"name": "retensi_permanen", "display_name": "Retensi Permanen", "data_type": "integer", "is_summable": False},
                {"name": "retensi_musnah", "display_name": "Retensi Musnah", "data_type": "integer", "is_summable": False},
                {"name": "naskah_ditindaklanjuti", "display_name": "Naskah Ditindaklanjuti", "data_type": "integer", "is_summable": False},
            ]
        )
        
        if result["status"] == "error":
            print(f" Error creating table: {result['message']}")
            # Try to get existing table if error
            table_def = table_service.get_default_table()
            if not table_def:
                print(" Critical: Could not get table definition.")
                return
            table_id = table_def["id"]
        else:
            table_id = result["data"]["id"]
            print(f" Table created with ID: {table_id}")
        
        # 2. Migrate Data
        print(" Migrating data from DataArsip...")
        
        # Helper to get columns
        cols = db.query(ColumnDefinition).filter(ColumnDefinition.table_id == table_id).all()
        
        # Query old data
        old_data = db.query(DataArsip).all()
        count = 0
        
        for item in old_data:
            # Construct JSON data
            json_data = {
                "naskah_masuk": item.naskah_masuk,
                "naskah_keluar": item.naskah_keluar,
                "disposisi": item.disposisi,
                "berkas": item.berkas,
                "retensi_permanen": item.retensi_permanen,
                "retensi_musnah": item.retensi_musnah,
                "naskah_ditindaklanjuti": item.naskah_ditindaklanjuti
            }
            
            # Check existing
            existing = db.query(DynamicData).filter(
                DynamicData.table_id == table_id,
                DynamicData.unit_kerja_id == item.unit_kerja_id,
                DynamicData.tanggal == item.tanggal
            ).first()
            
            if existing:
                print(f" Skipping duplicate for Unit {item.unit_kerja_id} Date {item.tanggal}")
                continue
                
            new_data = DynamicData(
                table_id=table_id,
                unit_kerja_id=item.unit_kerja_id,
                tanggal=item.tanggal,
                data=json_data
            )
            # Calculate total manually based on summable cols
            new_data.calculate_total(cols)
            
            db.add(new_data)
            count += 1
            
        db.commit()
        print(f" Migration complete. migrated {count} records.")

if __name__ == "__main__":
    migrate_data()
