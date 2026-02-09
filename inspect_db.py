from sqlalchemy import text, inspect
from app.database import engine

def inspect_db():
    insp = inspect(engine)
    tables = insp.get_table_names()
    print(f"Tables: {tables}")
    
    for t in tables:
        if 'arsip' in t:
            print(f"\nTable: {t}")
            indexes = insp.get_indexes(t)
            for idx in indexes:
                print(f"  Index: {idx['name']} -> {idx['column_names']}")

if __name__ == "__main__":
    inspect_db()
