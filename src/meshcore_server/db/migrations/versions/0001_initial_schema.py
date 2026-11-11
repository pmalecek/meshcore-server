"""initial schema

Revision ID: 0001
Revises: 
Create Date: 2026-11-11 09:43:16.307536
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

import meshcore_server.db.types


revision: str = '0001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('adverts',
    sa.Column('id', sa.BigInteger().with_variant(sa.Integer(), 'sqlite'), autoincrement=True, nullable=False),
    sa.Column('public_key', sa.String(length=64), nullable=False),
    sa.Column('name', sa.String(length=64), nullable=True),
    sa.Column('type', sa.Integer(), nullable=True),
    sa.Column('lat', sa.Float(), nullable=True),
    sa.Column('lon', sa.Float(), nullable=True),
    sa.Column('path_len', sa.Integer(), nullable=True),
    sa.Column('path', sa.String(length=255), nullable=True),
    sa.Column('snr', sa.Float(), nullable=True),
    sa.Column('received_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
    mysql_engine='InnoDB'
    )
    with op.batch_alter_table('adverts', schema=None) as batch_op:
        batch_op.create_index('ix_adverts_key_time', ['public_key', 'received_at'], unique=False)
        batch_op.create_index('ix_adverts_received_at', ['received_at'], unique=False)

    op.create_table('api_tokens',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('token_hash', sa.String(length=128), nullable=False),
    sa.Column('scopes', sa.String(length=64), nullable=False),
    sa.Column('created_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.Column('last_used_at', meshcore_server.db.types.UTCDateTime(), nullable=True),
    sa.Column('revoked_at', meshcore_server.db.types.UTCDateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name'),
    sa.UniqueConstraint('token_hash'),
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
    mysql_engine='InnoDB'
    )
    op.create_table('channels',
    sa.Column('idx', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('secret', sa.String(length=32), nullable=True),
    sa.Column('updated_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.PrimaryKeyConstraint('idx'),
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
    mysql_engine='InnoDB'
    )
    op.create_table('commands',
    sa.Column('id', sa.BigInteger().with_variant(sa.Integer(), 'sqlite'), autoincrement=True, nullable=False),
    sa.Column('type', sa.String(length=32), nullable=False),
    sa.Column('payload', meshcore_server.db.types.JSONText(), nullable=True),
    sa.Column('status', sa.String(length=16), nullable=False),
    sa.Column('error', sa.Text(), nullable=True),
    sa.Column('token_id', sa.Integer(), nullable=True),
    sa.Column('created_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.Column('started_at', meshcore_server.db.types.UTCDateTime(), nullable=True),
    sa.Column('finished_at', meshcore_server.db.types.UTCDateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
    mysql_engine='InnoDB'
    )
    with op.batch_alter_table('commands', schema=None) as batch_op:
        batch_op.create_index('ix_commands_status', ['status', 'created_at'], unique=False)

    op.create_table('contacts',
    sa.Column('public_key', sa.String(length=64), nullable=False),
    sa.Column('name', sa.String(length=64), nullable=True),
    sa.Column('type', sa.Integer(), nullable=True),
    sa.Column('flags', sa.Integer(), nullable=True),
    sa.Column('out_path', sa.String(length=255), nullable=True),
    sa.Column('out_path_len', sa.Integer(), nullable=True),
    sa.Column('lat', sa.Float(), nullable=True),
    sa.Column('lon', sa.Float(), nullable=True),
    sa.Column('last_advert_at', meshcore_server.db.types.UTCDateTime(), nullable=True),
    sa.Column('last_message_at', meshcore_server.db.types.UTCDateTime(), nullable=True),
    sa.Column('favorite', sa.Boolean(), nullable=False),
    sa.Column('on_device', sa.Boolean(), nullable=False),
    sa.Column('first_seen_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.Column('updated_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.PrimaryKeyConstraint('public_key'),
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
    mysql_engine='InnoDB'
    )
    with op.batch_alter_table('contacts', schema=None) as batch_op:
        batch_op.create_index('ix_contacts_name', ['name'], unique=False)

    op.create_table('device',
    sa.Column('id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('public_key', sa.String(length=64), nullable=False),
    sa.Column('name', sa.String(length=64), nullable=True),
    sa.Column('adv_type', sa.Integer(), nullable=True),
    sa.Column('model', sa.String(length=64), nullable=True),
    sa.Column('firmware_ver', sa.String(length=32), nullable=True),
    sa.Column('firmware_build', sa.String(length=32), nullable=True),
    sa.Column('protocol_ver', sa.Integer(), nullable=True),
    sa.Column('max_contacts', sa.Integer(), nullable=True),
    sa.Column('max_channels', sa.Integer(), nullable=True),
    sa.Column('capabilities', meshcore_server.db.types.JSONText(), nullable=True),
    sa.Column('transport', sa.String(length=16), nullable=True),
    sa.Column('transport_config', meshcore_server.db.types.JSONText(), nullable=True),
    sa.Column('first_seen_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.Column('last_connected_at', meshcore_server.db.types.UTCDateTime(), nullable=True),
    sa.Column('updated_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
    mysql_engine='InnoDB'
    )
    op.create_table('device_config',
    sa.Column('key', sa.String(length=64), nullable=False),
    sa.Column('value', meshcore_server.db.types.JSONText(), nullable=True),
    sa.Column('updated_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.PrimaryKeyConstraint('key'),
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
    mysql_engine='InnoDB'
    )
    op.create_table('events',
    sa.Column('seq', sa.BigInteger().with_variant(sa.Integer(), 'sqlite'), autoincrement=True, nullable=False),
    sa.Column('type', sa.String(length=64), nullable=False),
    sa.Column('data', meshcore_server.db.types.JSONText(), nullable=True),
    sa.Column('created_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.PrimaryKeyConstraint('seq'),
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
    mysql_engine='InnoDB'
    )
    with op.batch_alter_table('events', schema=None) as batch_op:
        batch_op.create_index('ix_events_created_at', ['created_at'], unique=False)

    op.create_table('messages',
    sa.Column('id', sa.BigInteger().with_variant(sa.Integer(), 'sqlite'), autoincrement=True, nullable=False),
    sa.Column('kind', sa.String(length=8), nullable=False),
    sa.Column('direction', sa.String(length=3), nullable=False),
    sa.Column('contact_prefix', sa.String(length=12), nullable=True),
    sa.Column('contact_key', sa.String(length=64), nullable=True),
    sa.Column('channel_idx', sa.Integer(), nullable=True),
    sa.Column('sender_name', sa.String(length=64), nullable=True),
    sa.Column('text', sa.Text(), nullable=False),
    sa.Column('txt_type', sa.Integer(), nullable=True),
    sa.Column('sender_ts', sa.BigInteger(), nullable=True),
    sa.Column('received_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.Column('path_len', sa.Integer(), nullable=True),
    sa.Column('path', sa.String(length=255), nullable=True),
    sa.Column('snr', sa.Float(), nullable=True),
    sa.Column('rssi', sa.Integer(), nullable=True),
    sa.Column('status', sa.String(length=16), nullable=False),
    sa.Column('expected_ack', sa.String(length=16), nullable=True),
    sa.Column('attempts', sa.Integer(), nullable=False),
    sa.Column('expires_at', meshcore_server.db.types.UTCDateTime(), nullable=True),
    sa.Column('command_id', sa.BigInteger(), nullable=True),
    sa.Column('dedup_hash', sa.String(length=64), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('dedup_hash'),
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
    mysql_engine='InnoDB'
    )
    with op.batch_alter_table('messages', schema=None) as batch_op:
        batch_op.create_index('ix_messages_channel', ['kind', 'channel_idx', 'received_at'], unique=False)
        batch_op.create_index('ix_messages_dm', ['kind', 'contact_prefix', 'received_at'], unique=False)
        batch_op.create_index('ix_messages_expected_ack', ['expected_ack'], unique=False)
        batch_op.create_index('ix_messages_received_at', ['received_at'], unique=False)

    op.create_table('read_state',
    sa.Column('reader', sa.String(length=128), nullable=False),
    sa.Column('conversation', sa.String(length=80), nullable=False),
    sa.Column('last_read_message_id', sa.BigInteger(), nullable=False),
    sa.Column('updated_at', meshcore_server.db.types.UTCDateTime(), nullable=False),
    sa.PrimaryKeyConstraint('reader', 'conversation'),
    mysql_charset='utf8mb4',
    mysql_collate='utf8mb4_unicode_ci',
    mysql_engine='InnoDB'
    )


def downgrade() -> None:
    op.drop_table('read_state')
    with op.batch_alter_table('messages', schema=None) as batch_op:
        batch_op.drop_index('ix_messages_received_at')
        batch_op.drop_index('ix_messages_expected_ack')
        batch_op.drop_index('ix_messages_dm')
        batch_op.drop_index('ix_messages_channel')

    op.drop_table('messages')
    with op.batch_alter_table('events', schema=None) as batch_op:
        batch_op.drop_index('ix_events_created_at')

    op.drop_table('events')
    op.drop_table('device_config')
    op.drop_table('device')
    with op.batch_alter_table('contacts', schema=None) as batch_op:
        batch_op.drop_index('ix_contacts_name')

    op.drop_table('contacts')
    with op.batch_alter_table('commands', schema=None) as batch_op:
        batch_op.drop_index('ix_commands_status')

    op.drop_table('commands')
    op.drop_table('channels')
    op.drop_table('api_tokens')
    with op.batch_alter_table('adverts', schema=None) as batch_op:
        batch_op.drop_index('ix_adverts_received_at')
        batch_op.drop_index('ix_adverts_key_time')

    op.drop_table('adverts')
