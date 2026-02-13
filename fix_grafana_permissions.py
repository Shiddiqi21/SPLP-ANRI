
from app.database import SessionLocal
from sqlalchemy import text

def fix_permissions():
    db = SessionLocal()
    try:
        print("Fixing usage permissions for 'grafana' user...")
        
        # 1. Check if user exists (Optional, but good safety)
        # We just try to Grant. If user doesn't exist, it might fail or create it depending on version.
        # Safer to Create if not exists then Grant.
        
        try:
            # Grant SELECT on database 'anri'
            # We assume database name is 'anri' based on error msg or .env
            
            # Create user if not exists (password might be issue if we overwrite, so just GRANT)
            # User already exists presumably if error is Access Denied.
            
            sql_grant = "GRANT SELECT ON anri.* TO 'grafana'@'%';"
            db.execute(text(sql_grant))
            print("GRANTED SELECT on anri.*")
            
            sql_flush = "FLUSH PRIVILEGES;"
            db.execute(text(sql_flush))
            print("FLUSHED PRIVILEGES")
            
            print("SUCCESS: Grafana user should now have access.")
            
        except Exception as e:
            print(f"SQL Error: {e}")
            
    except Exception as e:
        print(f"Connection Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    fix_permissions()
