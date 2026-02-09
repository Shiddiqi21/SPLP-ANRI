
from app.database import get_db_context
from app.models.table_models import TableDefinition

def check():
    with get_db_context() as db:
        count = db.query(TableDefinition).count()
        print(f"Registered Table Count: {count}")
        if count > 0:
            tables = db.query(TableDefinition).all()
            for t in tables:
                print(f" - {t.name} (Default: {t.is_default})")

if __name__ == "__main__":
    check()
