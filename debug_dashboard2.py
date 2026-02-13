
import sys
import os
sys.path.append(os.getcwd())

from app.database import SessionLocal
from app.models.arsip_models import Instansi

from app.models.table_models import TableDefinition
from sqlalchemy import text

def debug_query():
    db = SessionLocal()
    try:
        print("Debugging 500 Error Scenario...")
        
        # PARAMETERS from Screenshot
        exclude_meta = True
        include_total = True  # Default logic now
        use_display_name = True # Default
        month_filter = [1, 2, 3] # Sample from URL
        
        # 1. Fetch Data
        sql = """
            SELECT 
                t.month as bulan,
                'DummyMonth' as nama_bulan,
                COALESCE(SUM(t.total), 0) as `total`
            FROM data_arsip_monthly_summary t
            GROUP BY t.month
            ORDER BY t.month
        """
        result = db.execute(text(sql)).mappings().all()
        print(f"DB Result Count: {len(result)}")
        
        # 2. Process Logic (Mimic stats_routes.py lines 193-240)
        monthly_data = []
        col_mapping = {"total": "Total"}
        selected_cols = ["total"]
        
        for row in result:
             row_dict = dict(row)
             if use_display_name:
                 new_row = {}
                 if not exclude_meta:
                     new_row['Bulan'] = row_dict['bulan']
                     new_row['Nama Bulan'] = row_dict['nama_bulan']
                 
                 if include_total:
                     new_row['Total'] = row_dict.get('total', 0)
                     
                 # selected_cols loop...
                 
                 monthly_data.append(new_row)
        
        print(f"Processed Data Count: {len(monthly_data)}")
        if monthly_data:
             print(f"Sample Row: {monthly_data[0]}")
             
        # 3. Fill Missing Months Logic
        existing_months = {d.get('Bulan') or d.get('bulan') for d in monthly_data}
        print(f"Existing Months keys: {existing_months}")
        
        # WHEN exclude_meta=True, 'Bulan' is NOT in new_row!
        # So existing_months will be {None}.
        
        # The Logic:
        # existing_months = {d.get('Bulan') or d.get('bulan') for d in monthly_data}
        # d.get('Bulan') -> None
        # d.get('bulan') -> None
        # result -> {None}
        
        months_to_fill = month_filter
        for m in months_to_fill:
             if m not in existing_months: # m (int) is NOT None. True.
                 # Appending Zero Row...
                 pass
        
        # 4. Sorting Logic (THE CRASH SUSPECT)
        print("Attempting Sort...")
        try:
             monthly_data.sort(key=lambda x: x.get('Bulan') or x.get('bulan'))
             print("Sort SUCCESS")
        except Exception as e:
             print(f"Sort FAILED: {e}")

    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    debug_query()

if __name__ == "__main__":
    debug_query()
