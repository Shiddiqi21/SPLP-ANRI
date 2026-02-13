
from app.database import SessionLocal
from app.models.table_models import TableDefinition

def check_tables():
    db = SessionLocal()
    try:
        tables = db.query(TableDefinition).all()
        for t in tables:
            print(f"ID: {t.id} | Name: {t.name} | Display: {t.display_name}")
    finally:
        db.close()

if __name__ == "__main__":
    check_tables()
