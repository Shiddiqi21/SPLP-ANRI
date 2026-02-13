import requests, json

GRAFANA_URL = "http://localhost:3000"
GRAFANA_AUTH = ("admin", "admin")
DASHBOARD_UID = "adbcrvm"
MYSQL_UID = "ffcwafoxxaf40e" 

def fix_vars_sql():
    print(f"Fetching dashboard {DASHBOARD_UID}...")
    try:
        resp = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}", auth=GRAFANA_AUTH)
        if resp.status_code != 200:
            print(f"Failed: {resp.text}")
            return

        data = resp.json()
        dashboard = data["dashboard"]
        
        # 1. REMOVE existing variables
        original_vars = dashboard.get("templating", {}).get("list", [])
        new_vars = [v for v in original_vars if v["name"] not in ["instansi", "unit_kerja", "tahun", "bulan"]]
        
        # 2. DEFINITIONS (Corrected SQL)
        
        # Tahun Variable (Fix: Use YEAR(tanggal))
        var_tahun = {
          "datasource": {"type": "mysql", "uid": MYSQL_UID},
          "definition": "SELECT DISTINCT YEAR(tanggal) AS __value, YEAR(tanggal) AS __text FROM data_arsip ORDER BY 1 DESC",
          "label": "Tahun",
          "name": "tahun",
          "query": "SELECT DISTINCT YEAR(tanggal) AS __value, YEAR(tanggal) AS __text FROM data_arsip ORDER BY 1 DESC",
          "refresh": 1,
          "sort": 0,
          "type": "query",
          "multi": True,
          "includeAll": True,
          "allValue": None, # Default to sending all values
        }

        # Bulan Variable (Static is safer/faster)
        var_bulan = {
          "datasource": {"type": "mysql", "uid": MYSQL_UID},
           # Use static list for Month to avoid DB query overhead
          "query": "SELECT 1 AS __value, 'Januari' AS __text UNION SELECT 2, 'Februari' UNION SELECT 3, 'Maret' UNION SELECT 4, 'April' UNION SELECT 5, 'Mei' UNION SELECT 6, 'Juni' UNION SELECT 7, 'Juli' UNION SELECT 8, 'Agustus' UNION SELECT 9, 'September' UNION SELECT 10, 'Oktober' UNION SELECT 11, 'November' UNION SELECT 12, 'Desember'",
          "label": "Bulan",
          "name": "bulan",
          "refresh": 1,
          "sort": 0,
          "type": "query",
          "multi": True,
          "includeAll": True
        }

        # Instansi Variable
        var_instansi = {
          "datasource": {"type": "mysql", "uid": MYSQL_UID},
          "definition": "SELECT nama AS __text, id AS __value FROM instansi ORDER BY nama ASC",
          "label": "Instansi",
          "name": "instansi",
          "query": "SELECT nama AS __text, id AS __value FROM instansi ORDER BY nama ASC",
          "refresh": 1, # Refresh on Dashboard Load
          "sort": 1,
          "type": "query",
          "multi": True,
          "includeAll": True,
          "allValue": None, # Null = Send All Values (CSV)
        }

        # Unit Kerja Variable (Dependency)
        # Use $__all for safety if Instansi is "All"
        var_unit_kerja = {
          "datasource": {"type": "mysql", "uid": MYSQL_UID},
          "definition": "SELECT nama AS __text, id AS __value FROM unit_kerja WHERE instansi_id IN (${instansi})",
          "label": "Unit Kerja",
          "name": "unit_kerja",
          "query": "SELECT nama AS __text, id AS __value FROM unit_kerja WHERE instansi_id IN (${instansi})",
          "refresh": 1,
          "sort": 1,
          "type": "query",
          "multi": True,
          "includeAll": True,
          "allValue": None,
        }
        
        # Assemble
        final_vars = [var_tahun, var_instansi, var_unit_kerja, var_bulan] + new_vars
        dashboard["templating"]["list"] = final_vars
        
        print(f"Saving dashboard with CORRECTED SQL Variables...")
        payload = {
            "dashboard": dashboard,
            "message": "Fix Vars: Corrected SQL syntax for Jahr (YEAR(tanggal)) and UnitKerja",
            "overwrite": True
        }
        r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", auth=GRAFANA_AUTH, json=payload)
        print(r.text)
        
    except Exception as e:
        print(f"Script Error: {e}")

if __name__ == "__main__":
    fix_vars_sql()
