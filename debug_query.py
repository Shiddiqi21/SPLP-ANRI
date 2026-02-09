
import time
from app.services.table_service import table_service
from app.database import get_db_context
from sqlalchemy import text

def benchmark():
    print("--- Benchmarking Table Service ---")
    
    # 1. Total Count (Raw SQL)
    with get_db_context() as db:
        start = time.time()
        count = db.execute(text("SELECT COUNT(*) FROM data_arsip")).scalar()
        print(f"Raw COUNT(*): {count} rows. Time: {time.time() - start:.4f}s")
    
    # 2. Service Call (No Filters) - Should use cache if hot
    start = time.time()
    # Assuming table_id 1 is 'data_arsip'
    data = table_service.get_dynamic_data(table_id=1, limit=50, offset=0)
    print(f"Service Call (No Filter): {len(data.get('data',[]))} items. Total: {data.get('total')}. Time: {time.time() - start:.4f}s")
    
    # 3. Service Call (With Date Filter)
    start = time.time()
    # Mock date filter
    from datetime import date
    data = table_service.get_dynamic_data(table_id=1, tanggal_start=date(2024, 1, 1), limit=50)
    print(f"Service Call (Date Filter): {len(data.get('data',[]))} items. Time: {time.time() - start:.4f}s")

if __name__ == "__main__":
    benchmark()
