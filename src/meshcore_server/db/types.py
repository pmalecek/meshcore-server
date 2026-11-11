"""Typy sloupců, které se chovají stejně v SQLite i MySQL."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import BigInteger, DateTime, Integer, Text
from sqlalchemy.dialects import mysql
from sqlalchemy.types import TypeDecorator

# Autoincrement primární klíč: v SQLite musí být INTEGER, jinde BIGINT.
BigIntPK = BigInteger().with_variant(Integer, "sqlite")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class UTCDateTime(TypeDecorator):
    """Čas uložený jako DATETIME bez zóny, vždy v UTC.

    Do DB jde naivní UTC, z DB vrací datetime s tzinfo=UTC.
    """

    impl = DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect):
        # MySQL ukládá DATETIME bez zlomků sekundy; chceme mikrosekundy.
        if dialect.name == "mysql":
            return dialect.type_descriptor(mysql.DATETIME(fsp=6))
        return dialect.type_descriptor(DateTime())

    def process_bind_param(self, value: Optional[datetime], dialect) -> Optional[datetime]:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("UTCDateTime očekává datetime s časovou zónou")
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    def process_result_value(self, value: Optional[datetime], dialect) -> Optional[datetime]:
        if value is None:
            return None
        return value.replace(tzinfo=timezone.utc)


class JSONText(TypeDecorator):
    """JSON uložený jako TEXT (bez závislosti na JSON typu databáze)."""

    impl = Text
    cache_ok = True

    def process_bind_param(self, value: Any, dialect) -> Optional[str]:
        if value is None:
            return None
        return json.dumps(value, ensure_ascii=False, default=str)

    def process_result_value(self, value: Optional[str], dialect) -> Any:
        if value is None:
            return None
        return json.loads(value)
