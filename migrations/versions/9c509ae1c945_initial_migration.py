"""Initial migration

Revision ID: 9c509ae1c945
Revises: 
Create Date: 2026-01-27 14:33:10.439586

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9c509ae1c945'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create users table
    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('username', sa.String(50), nullable=False),
        sa.Column('email', sa.String(100), nullable=False),
        sa.Column('hashed_password', sa.String(255), nullable=False),
        sa.Column('full_name', sa.String(100), nullable=True),
        sa.Column('is_active', sa.Boolean(), default=True),
        sa.Column('is_admin', sa.Boolean(), default=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('username'),
        sa.UniqueConstraint('email')
    )
    op.create_index('ix_users_username', 'users', ['username'])
    
    # Create arsip_data table
    op.create_table(
        'arsip_data',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('tanggal', sa.Date(), nullable=False),
        sa.Column('role_id', sa.Integer(), nullable=False),
        sa.Column('jenis_arsip', sa.String(255), nullable=False),
        sa.Column('instansi_id', sa.Integer(), nullable=False),
        sa.Column('data_content', sa.JSON(), nullable=True),
        sa.Column('keterangan', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_arsip_data_tanggal', 'arsip_data', ['tanggal'])
    op.create_index('ix_arsip_data_role_id', 'arsip_data', ['role_id'])
    op.create_index('ix_arsip_data_instansi_id', 'arsip_data', ['instansi_id'])
    
    # Create arsip_summary table
    op.create_table(
        'arsip_summary',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('tanggal', sa.Date(), nullable=False),
        sa.Column('instansi_id', sa.Integer(), nullable=False),
        sa.Column('jenis_arsip', sa.String(255), nullable=False),
        sa.Column('role_id', sa.Integer(), nullable=False),
        sa.Column('total_count', sa.Integer(), default=0),
        sa.Column('last_updated', sa.DateTime(), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_arsip_summary_tanggal', 'arsip_summary', ['tanggal'])
    op.create_index('ix_arsip_summary_instansi_id', 'arsip_summary', ['instansi_id'])
    op.create_index('ix_arsip_summary_jenis_arsip', 'arsip_summary', ['jenis_arsip'])
    op.create_index('ix_arsip_summary_role_id', 'arsip_summary', ['role_id'])
    
    # Create daily_summary table
    op.create_table(
        'daily_summary',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('tanggal', sa.Date(), nullable=False),
        sa.Column('total_arsip', sa.Integer(), default=0),
        sa.Column('total_instansi', sa.Integer(), default=0),
        sa.Column('total_jenis', sa.Integer(), default=0),
        sa.Column('last_updated', sa.DateTime(), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tanggal')
    )
    op.create_index('ix_daily_summary_tanggal', 'daily_summary', ['tanggal'])


def downgrade() -> None:
    op.drop_table('daily_summary')
    op.drop_table('arsip_summary')
    op.drop_table('arsip_data')
    op.drop_table('users')
