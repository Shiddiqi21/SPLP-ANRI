import requests
import urllib.parse
import time

API_URL = "http://localhost:8000"

def debug_url():
    print("Testing API with suspected problematic params...")
    
    # Simulate the params from screenshot
    # instansi_id=%7B1%2C6%2C7%7D  -> {1,6,7}
    # year=2025
    
    params = {
        "table_id": "1",
        "year": "2025", # User screenshot showed 2025
        "months": "1,2,3,4,5,6,7,8,9,10,11,12",
        "instansi_id": "{1,6,7}", # Glob format testing coverage
        "use_display_name": "true",
        "exclude_meta": "true"
    }
    
    start_time = time.time()
    try:
        print(f"Sending Request to {API_URL}/api/stats/grafana/monthly...")
        print(f"Params: {params}")
        
        resp = requests.get(f"{API_URL}/api/stats/grafana/monthly", params=params, timeout=30)
        
        duration = time.time() - start_time
        print(f"Time Taken: {duration:.2f}s")
        print(f"Status: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            row_count = len(data) if isinstance(data, list) else 0
            print(f"Rows Returned: {row_count}")
            print("Response Sample:", str(data)[:200])
        else:
            print("Error Response:", resp.text)
            
    except requests.exceptions.Timeout:
        print("CRITICAL: Request Timed Out (>30s)!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_url()
