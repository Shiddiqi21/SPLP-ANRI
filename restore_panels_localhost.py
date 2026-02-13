import requests, json

GRAFANA_URL = "http://localhost:3000"
GRAFANA_AUTH = ("admin", "admin")
DASHBOARD_UID = "adbcrvm"
INFINITY_UID = "ffcon30k8e03ka" 
# Use LOCALHOST because variables used it and it worked.
# This implies Grafana is either on the host OR using host networking.
API_BASE_URL = "http://localhost:8000"

def restore_panels_localhost():
    print(f"Fetching dashboard {DASHBOARD_UID}...")
    try:
        resp = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}", auth=GRAFANA_AUTH)
        if resp.status_code != 200:
            print(f"Failed: {resp.text}")
            return

        data = resp.json()
        dashboard = data["dashboard"]
        
        panels = dashboard.get("panels", [])
        print(f"Restoring {len(panels)} panels to LOCALHOST API...")
        
        for p in panels:
            if p.get("type") in ["row", "text"]:
                continue
            
            print(f"  Restoring Panel: {p.get('title', 'Untitled')}")
            
            p["datasource"] = {"type": "yesoreyeram-infinity-datasource", "uid": INFINITY_UID}
            p["targets"] = [
                {
                    "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": INFINITY_UID},
                    "type": "json",
                    "source": "url",
                    "method": "GET",
                    # EXACT URL MATCH to what worked for variables
                    "url": f"{API_BASE_URL}/api/stats/grafana/monthly",
                    "url_options": {
                        "params": [
                            {"key": "instansi_id", "value": "${instansi}"},
                            {"key": "unit_kerja_id", "value": "${unit_kerja}"},
                            {"key": "year", "value": "${tahun}"},
                            {"key": "months", "value": "${bulan}"},
                            {"key": "exclude_meta", "value": "true"}
                        ]
                    },
                    "format": "table",
                    "root_selector": "",
                    "columns": [],
                    "refId": "A"
                }
            ]
            
        dashboard["panels"] = panels
        
        print(f"Saving dashboard with LOCALHOST PANELS...")
        payload = {
            "dashboard": dashboard,
            "message": "Start Panic Fix: Revert to Localhost URL",
            "overwrite": True
        }
        r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", auth=GRAFANA_AUTH, json=payload)
        print(f"Save Result: {r.status_code} {r.text}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    restore_panels_localhost()
