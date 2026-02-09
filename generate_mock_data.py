"""Generate millions of sample data for data_arsip table"""
import random
import time
from datetime import datetime, timedelta
from sqlalchemy import text
from app.database import get_db_context

def generate_data(target_total=2000000, batch_size=10000):
    print(f"Starting data generation. Target: {target_total:,} records.")
    
    with get_db_context() as db:
        # 1. Get all Unit Kerja IDs
        units = db.execute(text("SELECT id FROM unit_kerja")).scalars().all()
        if not units:
            print("No Unit Kerja found! Please seed first.")
            return
        print(f"Found {len(units)} Unit Kerja.")

        # 2. Get column names from column_definitions
        cols_res = db.execute(text("SELECT name FROM column_definitions WHERE table_id = 1")).scalars().all()
        summable_cols = list(cols_res)
        print(f"Columns: {summable_cols}")

        # 3. Check current count
        current_count = db.execute(text("SELECT COUNT(*) FROM data_arsip")).scalar()
        print(f"Current count: {current_count:,}")
        
        needed = target_total - current_count
        if needed <= 0:
            print("Target already reached!")
            return

        print(f"Generating {needed:,} new records...")
        
        # 4. Prepare SQL
        col_names = ["unit_kerja_id", "tanggal", "total", "created_at", "updated_at"] + summable_cols
        placeholders = [":uid", ":tgl", ":total", ":now", ":now"] + [f":{c}" for c in summable_cols]
        
        sql = f"INSERT INTO data_arsip ({', '.join(col_names)}) VALUES ({', '.join(placeholders)})"
        
        # 5. Generation Loop
        start_time = time.time()
        batch = []
        total_inserted = 0
        
        base_date = datetime.now()
        now = datetime.now()
        
        for i in range(needed):
            # Random Data
            row = {}
            row_total = 0
            
            row['uid'] = random.choice(units)
            # Random date within last 2 years
            rand_days = random.randint(0, 730)
            row['tgl'] = (base_date - timedelta(days=rand_days)).strftime("%Y-%m-%d")
            row['now'] = now
            
            for col in summable_cols:
                val = random.randint(0, 100)
                row[col] = val
                row_total += val
            
            row['total'] = row_total
            batch.append(row)
            
            if len(batch) >= batch_size:
                db.execute(text(sql), batch)
                db.commit()
                total_inserted += len(batch)
                elapsed = time.time() - start_time
                rate = total_inserted / elapsed if elapsed > 0 else 0
                eta = (needed - total_inserted) / rate if rate > 0 else 0
                print(f"Inserted {total_inserted:,}/{needed:,} ({(total_inserted/needed)*100:.1f}%) - {rate:.0f} rows/sec - ETA: {eta/60:.1f} min")
                batch = []

        # Final batch
        if batch:
            db.execute(text(sql), batch)
            db.commit()
            total_inserted += len(batch)
        
        duration = time.time() - start_time
        print(f"\n✓ Done! Inserted {total_inserted:,} records in {duration/60:.1f} minutes.")
        print(f"Average speed: {total_inserted/duration:.0f} rows/second")

if __name__ == "__main__":
    generate_data()
