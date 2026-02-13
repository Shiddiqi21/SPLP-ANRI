import os
import sys
from sqlalchemy import create_engine, text

# Adjust path to find app module
sys.path.append(os.getcwd())
from app.api.stats_routes import get_db_context

def add_date_index():
    print("Checking for 'tanggal' index on 'data_arsip'...")
    
    with get_db_context() as db:
        try:
            # Check existing indexes
            result = db.execute(text("SHOW INDEX FROM data_arsip"))
            indexes = result.fetchall()
            
            has_date_index = False
            for idx in indexes:
                # idx[2] is Key_name, idx[4] is Column_name
                # We look for a simple index on 'tanggal' or one where 'tanggal' is first
                if idx[4] == 'tanggal' and idx[3] == 1: # Seq_in_index == 1
                    print(f"Found existing index on 'tanggal': {idx[2]}")
                    has_date_index = True
                    break
            
            if not has_date_index:
                print("No optimal index for 'tanggal' found. Creating 'ix_data_arsip_tanggal'...")
                # Create Index
                db.execute(text("CREATE INDEX ix_data_arsip_tanggal ON data_arsip(tanggal)"))
                db.commit()
                print("SUCCESS: Index 'ix_data_arsip_tanggal' created!")
            else:
                print("Index already exists. Skipping.")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    add_date_index()
