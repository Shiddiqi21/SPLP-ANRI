import requests
import json
import time

URL = "http://localhost:8000/api/stats/grafana/monthly"

def check_simple():
    print("--- Testing API with Minimal Query ---")
    try:
        start = time.time()
        # Query only Month 1 (January) for 1 specific year.
        # This is slightly faster than full year scan, although YEAR() still runs on all rows.
        # But let's see if it connects at least.
        params = {
            "table_id": 1,
            "year": "2024", 
            "months": "1", # Only Jan
            "use_display_name": "true"
        }
        resp = requests.get(URL, params=params, timeout=10) # 10s timeout
        duration = time.time() - start
        
        print(f"Status Code: {resp.status_code}")
        print(f"Time Taken: {duration:.2f}s")
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"Data Rows: {len(data)}")
            if len(data) > 0:
                print("SUCCESS: Data retrieved!")
            else:
                print("WARNING: Empty data returned (Check DB content)")
        else:
            print(f"ERROR: Server returned {resp.status_code}")
            print(f"Response: {resp.text[:200]}")
            
    except requests.exceptions.ConnectionError:
        print("CRITICAL: Connection Refused. Server is DOWN.")
        print("Please restart `run.bat`")
    except requests.exceptions.Timeout:
        print("CRITICAL: Request Timed Out (Server is too slow).")
        print("This confirms the 'Original Code' cannot handle 2M rows.")
    except Exception as e:
        print(f"Expection: {e}")

if __name__ == "__main__":
    check_simple()
