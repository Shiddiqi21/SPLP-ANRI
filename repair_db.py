
from app.database import SessionLocal
from sqlalchemy import text

def repair_tables():
    db = SessionLocal()
    try:
        print("Attempting to REPAIR system tables due to Aria engine corruption...")
        
        # Identify key system tables for permissions
        tables = ["mysql.db", "mysql.user", "mysql.tables_priv", "mysql.columns_priv"]
        
        for t_name in tables:
            try:
                print(f"Repairing {t_name}...")
                db.execute(text(f"REPAIR TABLE {t_name} USE_FRM;"))
                print(f"Repair command sent for {t_name}")
            except Exception as e:
                print(f"Failed to repair {t_name}: {e}")
        
        # Also try FLUSH again
        try:
             db.execute(text("FLUSH PRIVILEGES;"))
             print("Permissions Flushed.")
        except Exception as e:
             print(f"Flush failed: {e}")
             
    except Exception as e:
        print(f"Connection Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    repair_tables()
