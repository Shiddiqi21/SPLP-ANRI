import requests
import json

GRAFANA_URL = "http://localhost:3000"
GRAFANA_AUTH = ("admin", "admin")
DASHBOARD_UID = "adbcrvm"
INFINITY_UID = "ffcon30k8e03ka" 
API_BASE_URL = "http://localhost:8000"

def sync_panels_fixed():
    print(f"Fetching dashboard {DASHBOARD_UID}...")
    try:
        resp = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}", auth=GRAFANA_AUTH)
        if resp.status_code != 200:
            print(f"Failed: {resp.text}")
            return

        data = resp.json()
        dashboard = data["dashboard"]
        
        panels = dashboard.get("panels", [])
        print(f"Syncing {len(panels)} panels to LOCALHOST API with CSV Format...")
        
        for p in panels:
            if p.get("type") in ["row", "text"]:
                continue
            
            print(f"  Syncing Panel: {p.get('title', 'Untitled')}")
            
            p["datasource"] = {"type": "yesoreyeram-infinity-datasource", "uid": INFINITY_UID}
            p["targets"] = [
                {
                    "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": INFINITY_UID},
                    "type": "json",
                    "source": "url",
                    "method": "GET",
                    "url": f"{API_BASE_URL}/api/stats/grafana/monthly",
                    "url_options": {
                        "params": [
                            # Use :csv to ensure Grafana sends "1,2,3" string instead of glob "{1,2,3}"
                            # This is safer for the strict int() parser in stats_routes
                            {"key": "instansi_id", "value": "${instansi:csv}"},
                            {"key": "unit_kerja_id", "value": "${unit_kerja:csv}"},
                            {"key": "year", "value": "${tahun:csv}"},
                            {"key": "months", "value": "${bulan:csv}"},
                            {"key": "exclude_meta", "value": "true"},
                            {"key": "table_id", "value": "1"} # Explicitly set table_id
                        ]
                    },
                    "format": "table",
                    "root_selector": "",
                    "columns": [],
                    "refId": "A"
                }
            ]
            
        dashboard["panels"] = panels
        
        # Also ensure TimeZone is browser to match user local time
        dashboard["timezone"] = "browser"
        
        print(f"Saving dashboard...")
        payload = {
            "dashboard": dashboard,
            "message": "Sync Panels to Localhost + CSV Variables",
            "overwrite": True
        }
        r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", auth=GRAFANA_AUTH, json=payload)
        print(f"Save Result: {r.status_code} {r.text}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    sync_panels_fixed()
