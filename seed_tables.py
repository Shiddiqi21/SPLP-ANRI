"""Seed table definitions and column definitions for testing"""
from app.database import engine
from sqlalchemy import text

def seed_table_metadata():
    with engine.connect() as conn:
        # Check if already seeded
        count = conn.execute(text('SELECT COUNT(*) FROM table_definitions')).scalar()
        if count > 0:
            print(f"Already seeded ({count} tables). Skipping.")
            return
        
        # 1. Insert table_definitions
        conn.execute(text('''
            INSERT INTO table_definitions (id, name, display_name, description, is_default, created_at, updated_at)
            VALUES (1, 'data_arsip', 'Data Arsip', 'Tabel default untuk data arsip', TRUE, NOW(), NOW())
        '''))
        print("✓ Table definition created")
        
        # 2. Insert column_definitions
        columns = [
            ('naskah_masuk', 'Naskah Masuk', 0),
            ('naskah_keluar', 'Naskah Keluar', 1),
            ('disposisi', 'Disposisi', 2),
            ('berkas', 'Berkas', 3),
            ('retensi_permanen', 'Retensi Permanen', 4),
            ('retensi_musnah', 'Retensi Musnah', 5),
            ('naskah_ditindaklanjuti', 'Naskah Ditindaklanjuti', 6),
        ]
        
        for name, display, order in columns:
            conn.execute(text("""
                INSERT INTO column_definitions (table_id, name, display_name, data_type, is_required, is_summable, `order`, created_at)
                VALUES (1, :name, :display, 'integer', FALSE, TRUE, :ord, NOW())
            """), {'name': name, 'display': display, 'ord': order})
        
        conn.commit()
        print(f"✓ {len(columns)} column definitions created")
        
        # Verify
        tables = conn.execute(text('SELECT COUNT(*) FROM table_definitions')).scalar()
        cols = conn.execute(text('SELECT COUNT(*) FROM column_definitions')).scalar()
        print(f"\nVerification: {tables} tables, {cols} columns")

if __name__ == "__main__":
    seed_table_metadata()
