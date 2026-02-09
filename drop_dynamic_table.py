
from sqlalchemy import create_engine, text, inspect
from app.config import get_settings

def drop_table():
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    print(f"Connecting to database: {settings.db_name}")
    try:
        # DB Agnostic Check
        inspector = inspect(engine)
        if inspector.has_table("dynamic_data"):
            print("Table 'dynamic_data' found. Dropping...")
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE dynamic_data"))
                conn.commit()
            print("SUCCESS: Table 'dynamic_data' dropped.")
        else:
            print("Table 'dynamic_data' does not exist.")
            
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    drop_table()
