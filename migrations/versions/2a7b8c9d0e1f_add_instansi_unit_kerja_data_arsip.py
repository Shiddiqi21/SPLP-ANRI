"""Add instansi, unit_kerja, and data_arsip tables

Revision ID: 2a7b8c9d0e1f
Revises: 9c509ae1c945
Create Date: 2026-02-03 09:26:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2a7b8c9d0e1f'
down_revision: Union[str, None] = '9c509ae1c945'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create instansi table
    op.create_table(
        'instansi',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('kode', sa.String(20), nullable=False),
        sa.Column('nama', sa.String(255), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('kode')
    )
    op.create_index('ix_instansi_kode', 'instansi', ['kode'])
    op.create_index('ix_instansi_nama', 'instansi', ['nama'])
    
    # Create unit_kerja table
    op.create_table(
        'unit_kerja',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('instansi_id', sa.Integer(), nullable=False),
        sa.Column('kode', sa.String(50), nullable=False),
        sa.Column('nama', sa.String(255), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['instansi_id'], ['instansi.id'], ondelete='CASCADE')
    )
    op.create_index('ix_unit_kerja_instansi_id', 'unit_kerja', ['instansi_id'])
    op.create_index('ix_unit_kerja_kode', 'unit_kerja', ['kode'])
    op.create_index('ix_unit_kerja_nama', 'unit_kerja', ['nama'])
    
    # Create data_arsip table
    op.create_table(
        'data_arsip',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('unit_kerja_id', sa.Integer(), nullable=False),
        sa.Column('tanggal', sa.Date(), nullable=False),
        sa.Column('naskah_masuk', sa.Integer(), default=0),
        sa.Column('naskah_keluar', sa.Integer(), default=0),
        sa.Column('disposisi', sa.Integer(), default=0),
        sa.Column('berkas', sa.Integer(), default=0),
        sa.Column('retensi_permanen', sa.Integer(), default=0),
        sa.Column('retensi_musnah', sa.Integer(), default=0),
        sa.Column('naskah_ditindaklanjuti', sa.Integer(), default=0),
        sa.Column('total', sa.Integer(), default=0),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['unit_kerja_id'], ['unit_kerja.id'], ondelete='CASCADE')
    )
    op.create_index('ix_data_arsip_unit_kerja_id', 'data_arsip', ['unit_kerja_id'])
    op.create_index('ix_data_arsip_tanggal', 'data_arsip', ['tanggal'])


def downgrade() -> None:
    op.drop_table('data_arsip')
    op.drop_table('unit_kerja')
    op.drop_table('instansi')
