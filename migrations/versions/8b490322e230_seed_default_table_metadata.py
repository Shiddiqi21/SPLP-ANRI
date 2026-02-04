"""Seed default table metadata

Revision ID: 8b490322e230
Revises: 1d97ff5a107f
Create Date: 2026-02-04 11:05:41.040489

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8b490322e230'
down_revision: Union[str, None] = '1d97ff5a107f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


from datetime import datetime
from sqlalchemy import table, column, Integer, String, Text, Boolean, DateTime

def upgrade() -> None:
    # Get table references
    table_defs = table('table_definitions',
        column('id', Integer),
        column('name', String),
        column('display_name', String),
        column('description', Text),
        column('is_default', Boolean),
        column('created_at', DateTime),
        column('updated_at', DateTime)
    )

    col_defs = table('column_definitions',
        column('id', Integer),
        column('table_id', Integer),
        column('name', String),
        column('display_name', String),
        column('data_type', String),
        column('is_required', Boolean),
        column('is_summable', Boolean),
        column('order', Integer),
        column('created_at', DateTime)
    )

    # 1. Insert Table Metadata
    # We use a known ID (e.g., 1) or let it auto-increment? 
    # Better to insert and not rely on specific ID, but for relationships we need ID.
    # Since this is a fresh migration for others, we can assume ID=1 or fetch it.
    # However, bulk_insert doesn't return IDs easily in all backends.
    # We will use explicit ID=1 for the default table to be safe and consistent.
    
    op.bulk_insert(table_defs, [
        {
            'id': 1,
            'name': 'data_arsip',
            'display_name': 'Data Arsip',
            'description': 'Tabel default untuk data arsip (Migrated)',
            'is_default': True,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
    ])

    # 2. Insert Columns
    columns = [
        {'name': 'naskah_masuk', 'display_name': 'Naskah Masuk', 'order': 0},
        {'name': 'naskah_keluar', 'display_name': 'Naskah Keluar', 'order': 1},
        {'name': 'disposisi', 'display_name': 'Disposisi', 'order': 2},
        {'name': 'berkas', 'display_name': 'Berkas', 'order': 3},
        {'name': 'retensi_permanen', 'display_name': 'Retensi Permanen', 'order': 4},
        {'name': 'retensi_musnah', 'display_name': 'Retensi Musnah', 'order': 5},
        {'name': 'naskah_ditindaklanjuti', 'display_name': 'Naskah Ditindaklanjuti', 'order': 6},
    ]

    col_rows = []
    for i, col in enumerate(columns):
        col_rows.append({
            'table_id': 1, # FK to table_definitions.id = 1
            'name': col['name'],
            'display_name': col['display_name'],
            'data_type': 'integer',
            'is_required': False,
            'is_summable': True,
            'order': col['order'],
            'created_at': datetime.utcnow()
        })

    op.bulk_insert(col_defs, col_rows)
    
    # 3. Adjust Sequence (Optional but good for Postgres)
    # op.execute("SELECT setval('table_definitions_id_seq', (SELECT MAX(id) FROM table_definitions));")


def downgrade() -> None:
    # Remove the default data
    op.execute("DELETE FROM column_definitions WHERE table_id = 1")
    op.execute("DELETE FROM table_definitions WHERE id = 1")
