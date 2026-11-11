"""Definice tabulek (SQLAlchemy Core).

Server obsluhuje vždy jeden companion, proto tabulky nenesou device_id.
Změny schématu se dělají novou migrací v db/migrations/versions.
"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
)

from .types import BigIntPK, JSONText, UTCDateTime

metadata = MetaData()

MYSQL_OPTS = {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_unicode_ci"}

# Companion – vždy jediný řádek s id = 1.
device = Table(
    "device", metadata,
    Column("id", Integer, primary_key=True, autoincrement=False),
    Column("public_key", String(64), nullable=False),
    Column("name", String(64)),
    Column("adv_type", Integer),
    Column("model", String(64)),
    Column("firmware_ver", String(32)),
    Column("firmware_build", String(32)),
    Column("protocol_ver", Integer),
    Column("max_contacts", Integer),
    Column("max_channels", Integer),
    Column("capabilities", JSONText),
    Column("transport", String(16)),
    Column("transport_config", JSONText),
    Column("first_seen_at", UTCDateTime, nullable=False),
    Column("last_connected_at", UTCDateTime),
    Column("updated_at", UTCDateTime, nullable=False),
    **MYSQL_OPTS,
)

# Nastavení rádia a nodu jako klíč–hodnota (hodnota v JSON).
device_config = Table(
    "device_config", metadata,
    Column("key", String(64), primary_key=True),
    Column("value", JSONText),
    Column("updated_at", UTCDateTime, nullable=False),
    **MYSQL_OPTS,
)

contacts = Table(
    "contacts", metadata,
    Column("public_key", String(64), primary_key=True),
    Column("name", String(64)),
    Column("type", Integer),
    Column("flags", Integer),
    Column("out_path", String(255)),
    Column("out_path_len", Integer),
    Column("lat", Float),
    Column("lon", Float),
    Column("last_advert_at", UTCDateTime),
    Column("last_message_at", UTCDateTime),
    Column("favorite", Boolean, nullable=False, default=False),
    Column("on_device", Boolean, nullable=False, default=False),
    Column("first_seen_at", UTCDateTime, nullable=False),
    Column("updated_at", UTCDateTime, nullable=False),
    Index("ix_contacts_name", "name"),
    **MYSQL_OPTS,
)

channels = Table(
    "channels", metadata,
    Column("idx", Integer, primary_key=True, autoincrement=False),
    Column("name", String(64), nullable=False),
    Column("secret", String(32)),
    Column("region", String(64)),   # flood scope kanálu (#plzen); NULL = výchozí region zařízení
    Column("updated_at", UTCDateTime, nullable=False),
    **MYSQL_OPTS,
)

messages = Table(
    "messages", metadata,
    Column("id", BigIntPK, primary_key=True, autoincrement=True),
    Column("kind", String(8), nullable=False),          # dm | channel | room
    Column("direction", String(3), nullable=False),     # in | out
    Column("contact_prefix", String(12)),               # DM: prvních 6 bajtů klíče (hex)
    Column("contact_key", String(64)),                  # DM: celý klíč, pokud je kontakt znám
    Column("author_prefix", String(8)),                 # room: prefix klíče skutečného autora (4 B hex)
    Column("channel_idx", Integer),                     # kanál
    Column("sender_name", String(64)),
    Column("text", Text, nullable=False),
    Column("txt_type", Integer),
    Column("sender_ts", BigInteger),                    # čas odesílatele (unix s)
    Column("received_at", UTCDateTime, nullable=False), # čas serveru
    Column("path_len", Integer),
    Column("path", String(255)),
    Column("snr", Float),
    Column("rssi", Integer),
    Column("status", String(16), nullable=False),       # received | pending | sent | acked | failed
    Column("expected_ack", String(16)),
    Column("attempts", Integer, nullable=False, default=0),
    Column("expires_at", UTCDateTime),
    Column("command_id", BigInteger),
    Column("error", Text),                              # důvod selhání odeslání
    Column("heard_repeats", Integer, nullable=False, default=0),  # kolikrát companion zaslechl opakování
    Column("heard_by", String(255)),                    # hashe repeaterů, od kterých ji zaslechl (čárkou)
    Column("dedup_hash", String(64), nullable=False, unique=True),
    Index("ix_messages_dm", "kind", "contact_prefix", "received_at"),
    Index("ix_messages_channel", "kind", "channel_idx", "received_at"),
    Index("ix_messages_received_at", "received_at"),
    Index("ix_messages_expected_ack", "expected_ack"),
    **MYSQL_OPTS,
)

adverts = Table(
    "adverts", metadata,
    Column("id", BigIntPK, primary_key=True, autoincrement=True),
    Column("public_key", String(64), nullable=False),
    Column("name", String(64)),
    Column("type", Integer),
    Column("lat", Float),
    Column("lon", Float),
    Column("path_len", Integer),
    Column("path", String(255)),
    Column("snr", Float),
    Column("received_at", UTCDateTime, nullable=False),
    Index("ix_adverts_key_time", "public_key", "received_at"),
    Index("ix_adverts_received_at", "received_at"),
    **MYSQL_OPTS,
)

commands = Table(
    "commands", metadata,
    Column("id", BigIntPK, primary_key=True, autoincrement=True),
    Column("type", String(32), nullable=False),
    Column("payload", JSONText),
    Column("status", String(16), nullable=False),       # pending | running | done | failed
    Column("error", Text),
    Column("token_id", Integer),
    Column("created_at", UTCDateTime, nullable=False),
    Column("started_at", UTCDateTime),
    Column("finished_at", UTCDateTime),
    Index("ix_commands_status", "status", "created_at"),
    **MYSQL_OPTS,
)

events = Table(
    "events", metadata,
    Column("seq", BigIntPK, primary_key=True, autoincrement=True),
    Column("type", String(64), nullable=False),
    Column("data", JSONText),
    Column("created_at", UTCDateTime, nullable=False),
    Index("ix_events_created_at", "created_at"),
    **MYSQL_OPTS,
)

read_state = Table(
    "read_state", metadata,
    Column("reader", String(128), primary_key=True),
    Column("conversation", String(80), primary_key=True),  # dm:<prefix> | ch:<idx>
    Column("last_read_message_id", BigInteger, nullable=False),
    Column("updated_at", UTCDateTime, nullable=False),
    **MYSQL_OPTS,
)

api_tokens = Table(
    "api_tokens", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(64), nullable=False, unique=True),
    Column("token_hash", String(128), nullable=False, unique=True),
    Column("scopes", String(64), nullable=False),
    Column("created_at", UTCDateTime, nullable=False),
    Column("last_used_at", UTCDateTime),
    Column("revoked_at", UTCDateTime),
    **MYSQL_OPTS,
)

# Přihlášení k room serverům a repeaterům.
remote_logins = Table(
    "remote_logins", metadata,
    Column("contact_key", String(64), primary_key=True),
    Column("password", Text),                           # jen při remember=true; nikdy do API ani exportu
    Column("auto_login", Boolean, nullable=False, default=False),  # přihlásit znovu po reconnectu
    Column("logged_in", Boolean, nullable=False, default=False),
    Column("is_admin", Boolean, nullable=False, default=False),
    Column("permissions", Integer),
    Column("last_login_at", UTCDateTime),
    Column("last_error", Text),
    Column("updated_at", UTCDateTime, nullable=False),
    **MYSQL_OPTS,
)

# Tabulky, které ukazuje `db info` a později /server.
MAIN_TABLES = ("messages", "contacts", "channels", "adverts", "commands", "events")
