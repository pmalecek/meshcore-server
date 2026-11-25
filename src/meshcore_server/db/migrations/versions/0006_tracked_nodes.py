"""tracked nodes

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-25 07:00:00
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

import meshcore_server.db.types


revision: str = '0006'
down_revision: Union[str, None] = '0005'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

OPTS = dict(mysql_charset='utf8mb4', mysql_collate='utf8mb4_unicode_ci', mysql_engine='InnoDB')


def upgrade() -> None:
    op.create_table('tracked_nodes',
    sa.Column('contact_key', sa.String(length=64), nullable=False),
    sa.Column('added_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.Column('next_poll_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.Column('last_poll_at', meshcore_server.db.types.UTCDateTime(), nullable=True),
    sa.Column('last_ok_at', meshcore_server.db.types.UTCDateTime(), nullable=True),
    sa.Column('failures', sa.Integer(), nullable=False, server_default='0'),
    sa.Column('last_error', sa.Text(), nullable=True),
    sa.Column('last_status', meshcore_server.db.types.JSONText(), nullable=True),
    sa.Column('last_telemetry', meshcore_server.db.types.JSONText(), nullable=True),
    sa.Column('updated_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.PrimaryKeyConstraint('contact_key'),
    **OPTS
    )
    op.create_table('node_samples',
    sa.Column('id', sa.BigInteger().with_variant(sa.Integer(), 'sqlite'), autoincrement=True, nullable=False),
    sa.Column('contact_key', sa.String(length=64), nullable=False),
    sa.Column('at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.Column('data', meshcore_server.db.types.JSONText(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    **OPTS
    )
    with op.batch_alter_table('node_samples', schema=None) as batch_op:
        batch_op.create_index('ix_node_samples_key_at', ['contact_key', 'at'], unique=False)
        batch_op.create_index('ix_node_samples_at', ['at'], unique=False)


def downgrade() -> None:
    op.drop_table('node_samples')
    op.drop_table('tracked_nodes')
