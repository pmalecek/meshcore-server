"""contact region

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-25 06:30:00
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = '0005'
down_revision: Union[str, None] = '0004'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table('contacts', schema=None) as batch_op:
        batch_op.add_column(sa.Column('region', sa.String(length=64), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('contacts', schema=None) as batch_op:
        batch_op.drop_column('region')
