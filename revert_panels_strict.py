import requests, json

GRAFANA_URL = "http://localhost:3000"
GRAFANA_AUTH = ("admin", "admin")
DASHBOARD_UID = "adbcrvm"
INFINITY_UID = "ffcon30k8e03ka" 
LAN_IP = "11.1.239.6" 

def revert_panels_to_strict_api():
    print(f"Fetching dashboard {DASHBOARD_UID}...")
    try:
        resp = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}", auth=GRAFANA_AUTH)
        if resp.status_code != 200:
            print(f"Failed: {resp.text}")
            return

        data = resp.json()
        dashboard = data["dashboard"]
        
        panels = dashboard.get("panels", [])
        print(f"Reverting {len(panels)} panels to ORIGINAL SYSTEM API...")
        
        for p in panels:
            if p.get("type") in ["row", "text"]:
                continue
            
            # Pie Chart & Stat Panels -> Point to API
            # URL: http://LAN_IP:8000/api/stats/grafana/monthly
            # Params MUST MATCH stats_routes.py exactly:
            # - instansi_id: ${instansi}
            # - unit_kerja_id: ${unit_kerja}
            # - year: ${tahun}
            # - months: ${bulan} (PLURAL!)
            
            print(f"  Reverting Panel: {p.get('title', 'Untitled')}")
            
            p["datasource"] = {"type": "yesoreyeram-infinity-datasource", "uid": INFINITY_UID}
            p["targets"] = [
                {
                    "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": INFINITY_UID},
                    "type": "json",
                    "source": "url",
                    "method": "GET",
                    # Use LAN IP to ensure connectivity from Docker
                    "url": f"http://{LAN_IP}:8000/api/stats/grafana/monthly",
                    "url_options": {
                        "params": [
                            {"key": "instansi_id", "value": "${instansi}"},
                            {"key": "unit_kerja_id", "value": "${unit_kerja}"},
                            # "year" strictly matches stats_routes.py line 26: year: Optional[str]
                            {"key": "year", "value": "${tahun}"},
                            # "months" strictly matches stats_routes.py line 28: months: Optional[str]
                            {"key": "months", "value": "${bulan}"}, 
                             # 'exclude_meta' for pie charts if title contains "Jenis" or similar?
                             # Let's add it for safety if needed, but maybe not critical.
                             # Actually, let's enable it for cleanliness.
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
        
        print(f"Saving dashboard with STRICT API PANELS...")
        payload = {
            "dashboard": dashboard,
            "message": "Revert: Strict System API Mapping (months, year)",
            "overwrite": True
        }
        r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", auth=GRAFANA_AUTH, json=payload)
        print(f"Save Result: {r.status_code} {r.text}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    revert_panels_to_strict_api()
