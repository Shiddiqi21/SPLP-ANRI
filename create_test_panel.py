import requests
import json

GRAFANA_URL = "http://localhost:3000"
GRAFANA_AUTH = ("admin", "admin")
API_URL = "http://localhost:8000"

def create_debug_dashboard():
    print("creating debug dashboard...")
    
    # Define a SIMPLE panel with NO variables (Hardcoded)
    panel = {
        "id": 1,
        "title": "TEST PANEL (HARDCODED)",
        "type": "table",
        "datasource": {"type": "yesoreyeram-infinity-datasource"},
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
        "targets": [
            {
                "datasource": {"type": "yesoreyeram-infinity-datasource"},
                "type": "json",
                "source": "url",
                "method": "GET",
                # Hardcoded params to bypass variable issues
                "url": f"{API_URL}/api/stats/grafana/monthly",
                "url_options": {
                    "params": [
                        {"key": "table_id", "value": "1"},
                        {"key": "year", "value": "2024"}, # Adjust to a year known to have data
                        {"key": "months", "value": "1,2,3,4,5,6,7,8,9,10,11,12"},
                        {"key": "use_display_name", "value": "true"}
                    ]
                },
                "format": "table",
                "root_selector": "",
                "columns": [],
                "refId": "A"
            }
        ]
    }
    
    dashboard = {
        "id": None,
        "uid": "debug_panel",
        "title": "DEBUG VISUALIZATION",
        "tags": ["debug"],
        "timezone": "browser",
        "schemaVersion": 6,
        "version": 0,
        "panels": [panel]
    }
    
    payload = {
        "dashboard": dashboard,
        "overwrite": True
    }
    
    try:
        r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", auth=GRAFANA_AUTH, json=payload)
        print(f"Status: {r.status_code}")
        print(f"Response: {r.text}")
        if r.status_code == 200:
            print("\nSUCCESS! created dashboard 'DEBUG VISUALIZATION'")
            print(f"Go to: {GRAFANA_URL}/d/debug_panel")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_debug_dashboard()
