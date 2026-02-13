import requests, json

GRAFANA_URL = "http://localhost:3000"
GRAFANA_AUTH = ("admin", "admin")
DASHBOARD_UID = "adbcrvm"
MYSQL_UID = "ffcwafoxxaf40e" 

def switch_panels_to_mysql():
    print(f"Fetching dashboard {DASHBOARD_UID}...")
    try:
        resp = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}", auth=GRAFANA_AUTH)
        if resp.status_code != 200:
            print(f"Failed: {resp.text}")
            return

        data = resp.json()
        dashboard = data["dashboard"]
        
        panels = dashboard.get("panels", [])
        print(f"Updating {len(panels)} panels to MySQL Direct...")
        
        for p in panels:
            # Only update Visualization Panels (Pie, Bar, Stat)
            # Skip Rows or Texts
            if p.get("type") in ["row", "text"]:
                continue
            
            print(f"  Updating Panel: {p.get('title', 'Untitled')} ({p.get('type')})")
            
            # Construct Efficient SQL
            # Unit Kerja Filter:
            # If "All" is selected in Instansi, Unit Kerja dropdown might return "All"
            # But in our variable definition, we set `includeAll: true`.
            # Grafana formats `${unit_kerja}` as `'id1','id2'` automatically.
            
            where_clause = "WHERE 1=1"
            
            # If Unit Kerja is not 'All', filter by it.
            # If Instansi is 'All', usually Unit Kerja is 'All'.
            # Safest is: WHERE unit_kerja_id IN (${unit_kerja}) AND YEAR(tanggal) IN (${tahun})
            
            where_clause += " AND unit_kerja_id IN (${unit_kerja})"
            where_clause += " AND YEAR(tanggal) IN (${tahun})"
            
            # Month Filter
            where_clause += " AND MONTH(tanggal) IN (${bulan})"

            # Aggregate Query
            sql = """
            SELECT 
                SUM(naskah_masuk) as 'Naskah Masuk',
                SUM(naskah_keluar) as 'Naskah Keluar',
                SUM(disposisi) as 'Disposisi',
                SUM(retensi_musnah) as 'Retensi Musnah',
                SUM(naskah_ditindaklanjuti) as 'Naskah Ditindaklanjuti',
                SUM(berkas) as 'Berkas',
                SUM(retensi_permanen) as 'Retensi Permanen'
            FROM data_arsip
            """ + where_clause
            
            # Configure Target
            p["datasource"] = {"type": "mysql", "uid": MYSQL_UID}
            p["targets"] = [
                {
                    "datasource": {"type": "mysql", "uid": MYSQL_UID},
                    "format": "table",
                    "group": [],
                    "metricColumn": "none",
                    "rawQuery": True,
                    "rawSql": sql,
                    "refId": "A",
                    "select": [[{"params": ["value"], "type": "column"}]],
                    "table": "data_arsip",
                    "timeColumn": "tanggal",
                    "where": []
                }
            ]
            
        dashboard["panels"] = panels
        
        print(f"Saving dashboard with MYSQL PANELS...")
        payload = {
            "dashboard": dashboard,
            "message": "Performance: Switch Panels to Direct MySQL (Zero API Latency)",
            "overwrite": True
        }
        r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", auth=GRAFANA_AUTH, json=payload)
        print(f"Save Result: {r.status_code} {r.text}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    switch_panels_to_mysql()
