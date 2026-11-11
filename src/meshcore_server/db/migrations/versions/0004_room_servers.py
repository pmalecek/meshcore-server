"""room servers

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-11 11:42:28.222145
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

import meshcore_server.db.types


revision: str = '0004'
down_revision: Union[str, None] = '0003'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('remote_logins',
    sa.Column('contact_key', sa.String(length=64), nullable=False),
    sa.Column('password', sa.Text(), nullable=True),
    sa.Column('auto_login', sa.Boolean(), nullable=False, server_default=sa.false()),
    sa.Column('logged_in', sa.Boolean(), nullable=False, server_default=sa.false()),
    sa.Column('is_admin', sa.Boolean(), nullable=False, server_default=sa.false()),
    sa.Column('permissions', sa.Integer(), nullable=True),
    sa.Column('last_login_at', meshcore_server.db.types.UTCDateTime(), nullable=True),
    sa.Column('last_error', sa.Text(), nullable=True),
    sa.Column('updated_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.PrimaryKeyConstraint('contact_key'),
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
    mysql_engine='InnoDB'
    )
    with op.batch_alter_table('messages', schema=None) as batch_op:
        batch_op.add_column(sa.Column('author_prefix', sa.String(length=8), nullable=True))



    # Zprávy z room serverů, které dřív přišly jako DM, převést na samostatný typ.
    op.execute(
        "UPDATE messages SET kind = 'room' WHERE kind = 'dm' AND contact_key IN "
        "(SELECT public_key FROM contacts WHERE type = 3)"
    )


def downgrade() -> None:
    op.execute("UPDATE messages SET kind = 'dm' WHERE kind = 'room'")
    with op.batch_alter_table('messages', schema=None) as batch_op:
        batch_op.drop_column('author_prefix')

    op.drop_table('remote_logins')
