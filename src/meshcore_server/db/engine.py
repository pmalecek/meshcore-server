"""Připojení k databázi a migrace."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from urllib.parse import quote_plus

from sqlalchemy import create_engine, event, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from ..config import DatabaseConfig
from . import schema

logger = logging.getLogger("meshcore_server.db")

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def build_urls(cfg: DatabaseConfig) -> tuple[str, str]:
    """Vrátí (async URL pro běh serveru, sync URL pro migrace)."""
    if cfg.driver == "sqlite":
        path = Path(cfg.sqlite_path).expanduser().resolve()
        return f"sqlite+aiosqlite:///{path}", f"sqlite:///{path}"

    m = cfg.mysql
    auth = f"{quote_plus(m.user)}:{quote_plus(m.password)}@" if m.password else f"{quote_plus(m.user)}@"
    tail = f"{auth}{m.host}:{m.port}/{m.database}?charset=utf8mb4"
    return f"mysql+aiomysql://{tail}", f"mysql+pymysql://{tail}"


def _sqlite_pragmas(dbapi_conn, _record) -> None:
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")      # čtení nečeká na zápis
    cur.execute("PRAGMA synchronous=NORMAL")
    cur.execute("PRAGMA foreign_keys=ON")
    cur.execute("PRAGMA busy_timeout=5000")
    cur.close()


class Database:
    def __init__(self, cfg: DatabaseConfig) -> None:
        self.cfg = cfg
        self.url, self.sync_url = build_urls(cfg)
        self.engine: AsyncEngine | None = None

    @property
    def is_sqlite(self) -> bool:
        return self.cfg.driver == "sqlite"

    def describe(self) -> str:
        if self.is_sqlite:
            return f"SQLite {Path(self.cfg.sqlite_path).expanduser().resolve()}"
        m = self.cfg.mysql
        return f"MySQL {m.user}@{m.host}:{m.port}/{m.database}"

    def _prepare_sqlite_dir(self) -> None:
        if self.is_sqlite:
            Path(self.cfg.sqlite_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

    async def connect(self) -> None:
        self._prepare_sqlite_dir()
        if self.is_sqlite:
            self.engine = create_async_engine(self.url)
            event.listen(self.engine.sync_engine, "connect", _sqlite_pragmas)
        else:
            self.engine = create_async_engine(self.url, pool_pre_ping=True, pool_recycle=3600)
        async with self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("Databáze: %s", self.describe())

    async def close(self) -> None:
        if self.engine is not None:
            await self.engine.dispose()
            self.engine = None

    # ------------------------------------------------------------ migrace

    def _alembic_config(self):
        from alembic.config import Config as AlembicConfig

        cfg = AlembicConfig()
        cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
        cfg.set_main_option("sqlalchemy.url", self.sync_url.replace("%", "%%"))
        return cfg

    def migrate_sync(self) -> None:
        """Převede schéma na nejnovější verzi (blokující volání)."""
        from alembic import command

        self._prepare_sqlite_dir()
        engine = create_engine(self.sync_url)
        if self.is_sqlite:
            event.listen(engine, "connect", _sqlite_pragmas)
        try:
            with engine.begin() as conn:
                cfg = self._alembic_config()
                cfg.attributes["connection"] = conn
                command.upgrade(cfg, "head")
        finally:
            engine.dispose()

    async def migrate(self) -> None:
        await asyncio.to_thread(self.migrate_sync)

    def current_revision_sync(self) -> str | None:
        from alembic.runtime.migration import MigrationContext

        engine = create_engine(self.sync_url)
        try:
            with engine.connect() as conn:
                return MigrationContext.configure(conn).get_current_revision()
        finally:
            engine.dispose()

    # --------------------------------------------------------- statistiky

    async def table_counts(self) -> dict[str, int]:
        assert self.engine is not None
        result = {}
        async with self.engine.connect() as conn:
            for name in schema.MAIN_TABLES:
                table = schema.metadata.tables[name]
                result[name] = (await conn.execute(select(func.count()).select_from(table))).scalar_one()
        return result

    async def stats(self) -> dict:
        """Typ a verze DB, velikost a velikost hlavních tabulek (pro /server)."""
        assert self.engine is not None
        counts = await self.table_counts()
        tables = {name: {"rows": n, "bytes": None} for name, n in counts.items()}
        result = {"driver": self.cfg.driver, "version": None, "size_bytes": None,
                  "size_method": None, "schema_revision": None, "tables": tables}
        async with self.engine.connect() as conn:
            try:
                result["schema_revision"] = (await conn.execute(
                    text("SELECT version_num FROM alembic_version"))).scalar()
            except Exception:  # noqa: BLE001
                pass
            if self.is_sqlite:
                result["version"] = (await conn.execute(text("SELECT sqlite_version()"))).scalar()
                await self._sqlite_sizes(conn, result)
            else:
                result["version"] = (await conn.execute(text("SELECT VERSION()"))).scalar()
                rows = await conn.execute(text(
                    "SELECT TABLE_NAME, DATA_LENGTH + INDEX_LENGTH FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE()"
                ))
                total = 0
                for name, size in rows:
                    total += int(size or 0)
                    if name in tables:
                        tables[name]["bytes"] = int(size or 0)
                result["size_bytes"] = total
                result["size_method"] = "information_schema"
        return result

    async def _sqlite_sizes(self, conn, result: dict) -> None:
        path = Path(self.cfg.sqlite_path).expanduser().resolve()
        size = 0
        for suffix in ("", "-wal"):
            f = Path(str(path) + suffix)
            if f.exists():
                size += f.stat().st_size
        result["size_bytes"] = size
        result["size_method"] = "file"
        try:
            # dbstat je virtuální tabulka, která v některých buildech SQLite chybí.
            index_owner = {
                name: tbl for name, tbl in await conn.execute(
                    text("SELECT name, tbl_name FROM sqlite_master WHERE type = 'index'"))
            }
            per_table: dict[str, int] = {}
            for name, pgsize in await conn.execute(text("SELECT name, SUM(pgsize) FROM dbstat GROUP BY name")):
                owner = index_owner.get(name, name)
                per_table[owner] = per_table.get(owner, 0) + int(pgsize or 0)
        except Exception:  # noqa: BLE001
            return
        for name, info in result["tables"].items():
            info["bytes"] = per_table.get(name, 0)
        result["size_method"] = "dbstat"
