import time
from app.services.table_service import table_service
from app.database import get_db_context
from sqlalchemy import text
import traceback

def benchmark():
    print("--- Starting Benchmark ---")
    
    # 1. DB Connection
    t0 = time.time()
    try:
        with get_db_context() as db:
            db.execute(text("SELECT 1")).scalar()
    except Exception as e:
        print(f"DB Connect Failed: {e}")
        return
    t1 = time.time()
    print(f"1. Connect & Select 1: {(t1-t0)*1000:.2f} ms")
    
    # 2. Get Statistics
    try:
        start = time.time()
        stats = table_service.get_statistics(1)
        end = time.time()
        print(f"2. Get Statistics: {(end-start)*1000:.2f} ms")
        print(f"   Result: {stats}")
    except Exception as e:
        print(f"   Stats Error: {e}")
        traceback.print_exc()

    # 3. Get Data (First page)
    try:
        start = time.time()
        data = table_service.get_dynamic_data(table_id=1, limit=10, offset=0)
        end = time.time()
        print(f"3. Get Data (Page 1): {(end-start)*1000:.2f} ms")
        print(f"   Total Records: {data.get('total')}")
    except Exception as e:
        print(f"   Data Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    benchmark()
