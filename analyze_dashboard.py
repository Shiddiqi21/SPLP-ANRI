import requests, json

GRAFANA_URL = "http://localhost:3000"
GRAFANA_AUTH = ("admin", "admin")
DASHBOARD_UID = "adbcrvm"

def analyze_dashboard():
    print(f"Fetching dashboard {DASHBOARD_UID}...")
    try:
        resp = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}", auth=GRAFANA_AUTH)
        if resp.status_code != 200:
            print(f"Failed: {resp.text}")
            return

        data = resp.json()
        dashboard = data["dashboard"]
        
        print(f"Dashboard Title: {dashboard.get('title')}")
        
        panels = dashboard.get("panels", [])
        print(f"Found {len(panels)} panels.")
        
        for p in panels:
            title = p.get("title", "Untitled")
            p_type = p.get("type", "unknown")
            targets = p.get("targets", [])
            
            print(f"\n--- Panel: {title} ({p_type}) ---")
            for t in targets:
                # Raw SQL or Builder?
                raw_sql = t.get("rawSql", "")
                if raw_sql:
                    print(f"SQL: {raw_sql}")
                else:
                    print(f"Target: {json.dumps(t, indent=2)}")
                    
            # Check for Row panels (nested panels)
            if p_type == "row":
                sub_panels = p.get("panels", [])
                for sub in sub_panels:
                    s_title = sub.get("title", "Untitled")
                    s_targets = sub.get("targets", [])
                    print(f"\n  >> Sub-Panel: {s_title}")
                    for st in s_targets:
                         print(f"  SQL: {st.get('rawSql', '')}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_dashboard()
