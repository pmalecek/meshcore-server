"""heard repeats

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-11 10:43:17.153131
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

import meshcore_server.db.types


revision: str = '0003'
down_revision: Union[str, None] = '0002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table('messages', schema=None) as batch_op:
        batch_op.add_column(sa.Column('heard_repeats', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('heard_by', sa.String(length=255), nullable=True))



def downgrade() -> None:
    with op.batch_alter_table('messages', schema=None) as batch_op:
        batch_op.drop_column('heard_by')
        batch_op.drop_column('heard_repeats')

