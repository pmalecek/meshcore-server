"""channel region, message error

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-11 10:14:34.788948
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

import meshcore_server.db.types


revision: str = '0002'
down_revision: Union[str, None] = '0001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table('channels', schema=None) as batch_op:
        batch_op.add_column(sa.Column('region', sa.String(length=64), nullable=True))

    with op.batch_alter_table('messages', schema=None) as batch_op:
        batch_op.add_column(sa.Column('error', sa.Text(), nullable=True))



def downgrade() -> None:
    with op.batch_alter_table('messages', schema=None) as batch_op:
        batch_op.drop_column('error')

    with op.batch_alter_table('channels', schema=None) as batch_op:
        batch_op.drop_column('region')

