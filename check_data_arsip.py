
from app.database import get_db_context
from app.models.table_models import TableDefinition
from sqlalchemy import text

def check_data():
    with get_db_context() as db:
        # Check Row Count
        try:
            count = db.execute(text("SELECT COUNT(*) FROM data_arsip")).scalar()
            print(f"Data Arsip Row Count: {count}")
        except Exception as e:
            print(f"Error counting rows: {e}")

        # Check Metadata Columns
        table = db.query(TableDefinition).filter(TableDefinition.name == 'data_arsip').first()
        if table:
            print(f"Metadata Columns for {table.name}:")
            for col in table.columns:
                print(f" - {col.name} ({col.data_type})")
        else:
            print("Table definition for 'data_arsip' not found.")

if __name__ == "__main__":
    check_data()
