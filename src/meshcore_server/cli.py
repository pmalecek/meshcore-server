"""Příkazová řádka: meshcore-server <příkaz>."""

from __future__ import annotations

import argparse
import asyncio
import sys

from . import __version__
from .config import ConfigError, load_config
from .logging_setup import setup_logging


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="meshcore-server",
        description="Samostatný server pro MeshCore companion.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "-c", "--config",
        help="cesta ke konfiguračnímu souboru (jinak MCS_CONFIG, ./config.yaml, /etc/meshcore-server/config.yaml)",
    )
    sub = parser.add_subparsers(dest="command", metavar="<příkaz>")
    sub.add_parser("run", help="spustí server (výchozí příkaz)")
    sub.add_parser("check-config", help="ověří konfiguraci a vypíše výsledné hodnoty")

    db = sub.add_parser("db", help="správa databáze")
    db_sub = db.add_subparsers(dest="db_command", metavar="<akce>", required=True)
    db_sub.add_parser("migrate", help="převede schéma na nejnovější verzi")
    db_sub.add_parser("info", help="vypíše typ databáze, verzi schématu a počty záznamů")
    purge = db_sub.add_parser("purge", help="pročistí historii podle database.retention v konfiguraci")
    purge.add_argument("--dry-run", action="store_true", help="jen vypsat, kolik by se smazalo")
    purge.add_argument("--messages-days", type=int, help="přepíše database.retention.messages_days")
    purge.add_argument("--adverts-days", type=int, help="přepíše database.retention.adverts_days")
    purge.add_argument("--commands-days", type=int, help="přepíše database.retention.commands_days")
    backup = db_sub.add_parser("backup", help="záloha SQLite databáze (lze i za běhu serveru)")
    backup.add_argument("target", nargs="?", help="cílový soubor (výchozí backups/meshcore-<datum>.db)")

    exp = sub.add_parser("export", help="export kontaktů a kanálů do JSON (obsahuje klíče kanálů!)")
    exp.add_argument("target", nargs="?", help="cílový soubor (jinak na standardní výstup)")

    tok = sub.add_parser("token", help="správa API tokenů")
    tok_sub = tok.add_subparsers(dest="token_command", metavar="<akce>", required=True)
    create = tok_sub.add_parser("create", help="vytvoří token (zobrazí se jen jednou)")
    create.add_argument("name", help="název tokenu, např. meshcore-klient")
    create.add_argument("--scope", choices=["read", "write", "admin"], default="read",
                        help="oprávnění: read < write < admin (výchozí read)")
    tok_sub.add_parser("list", help="vypíše tokeny")
    revoke = tok_sub.add_parser("revoke", help="zneplatní token")
    revoke.add_argument("name")
    return parser


def _cmd_check_config(cfg, path) -> int:
    print(f"Konfigurace: {path if path else 'bez souboru (výchozí hodnoty + proměnné prostředí)'}")
    data = cfg.model_dump()
    if data["database"]["mysql"]["password"]:
        data["database"]["mysql"]["password"] = "***"
    if data["companion"]["ble"]["pin"]:
        data["companion"]["ble"]["pin"] = "***"

    def dump(node, indent=0):
        for key, value in node.items():
            if isinstance(value, dict):
                print("  " * indent + f"{key}:")
                dump(value, indent + 1)
            else:
                shown = "" if value is None else value
                print("  " * indent + f"{key}: {shown}")

    dump(data)
    print("\nKonfigurace je v pořádku.")
    return 0


def _cmd_run(cfg, path) -> int:
    from .app import Application  # import až zde, ať ostatní příkazy nic nepřipojují

    setup_logging(cfg.logging.level)

    async def _main() -> None:
        app = Application(cfg, path)
        await app.run()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
    return 0


def _cmd_db(cfg, action: str, args=None) -> int:
    from .db import Database

    setup_logging("WARNING")
    db = Database(cfg.database)

    if action == "backup":
        return _db_backup(cfg, db, args.target)
    if action == "purge":
        return _db_purge(cfg, db, args)

    if action == "migrate":
        print(f"Databáze: {db.describe()}")
        db.migrate_sync()
        print(f"Schéma je aktuální (verze {db.current_revision_sync()}).")
        return 0

    async def _info() -> dict:
        await db.connect()
        try:
            return await db.table_counts()
        finally:
            await db.close()

    print(f"Databáze: {db.describe()}")
    revision = db.current_revision_sync()
    if revision is None:
        print("Schéma zatím nebylo vytvořeno (spusťte `meshcore-server db migrate`).")
        return 1
    print(f"Verze schématu: {revision}")
    for table, count in asyncio.run(_info()).items():
        print(f"  {table:<10} {count:>8}")
    return 0


def _db_backup(cfg, db, target) -> int:
    import sqlite3
    from datetime import datetime
    from pathlib import Path

    if not db.is_sqlite:
        m = cfg.database.mysql
        print("Pro MySQL použijte mysqldump, např.:", file=sys.stderr)
        print(f"  mysqldump --single-transaction -h {m.host} -P {m.port} -u {m.user} -p {m.database} "
              f"> meshcore-$(date +%F).sql", file=sys.stderr)
        return 1
    src = Path(cfg.database.sqlite_path).expanduser().resolve()
    if not src.exists():
        print(f"Databáze {src} neexistuje.", file=sys.stderr)
        return 1
    dest = Path(target) if target else Path("backups") / f"meshcore-{datetime.now():%Y%m%d-%H%M%S}.db"
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Online backup API SQLite: konzistentní kopie i za běhu serveru (na rozdíl od kopírování souboru).
    with sqlite3.connect(src) as source, sqlite3.connect(dest) as copy:
        source.backup(copy)
    dest.chmod(0o600)
    print(f"Záloha uložena: {dest} ({dest.stat().st_size // 1024} kB)")
    return 0


def _db_purge(cfg, db, args) -> int:
    from .db.repo import Repository
    from .db.types import utcnow

    r = cfg.database.retention
    days = {
        "messages_days": args.messages_days if args.messages_days is not None else r.messages_days,
        "adverts_days": args.adverts_days if args.adverts_days is not None else r.adverts_days,
        "commands_days": args.commands_days if args.commands_days is not None else r.commands_days,
    }
    if not any(days.values()):
        print("Pročišťování není nastavené (database.retention je 0); zadejte např. --messages-days 365.")
        return 0

    async def _run():
        await db.connect()
        try:
            return await Repository(db.engine).purge(utcnow(), dry_run=args.dry_run, **days)
        finally:
            await db.close()

    result = asyncio.run(_run())
    verb = "Smazalo by se" if args.dry_run else "Smazáno"
    for table, count in result.items():
        print(f"  {verb}: {table} {count} (starší než {days[table + '_days']} dní)")
    return 0


def _cmd_export(cfg, target) -> int:
    import json
    import os
    from pathlib import Path

    from .db import Database
    from .db.repo import Repository
    from .transfer import build_export

    setup_logging("WARNING")
    db = Database(cfg.database)

    async def _run():
        await db.connect()
        try:
            return await build_export(Repository(db.engine))
        finally:
            await db.close()

    data = json.dumps(asyncio.run(_run()), ensure_ascii=False, indent=2)
    if not target:
        print(data)
        return 0
    path = Path(target)
    # Obsahuje klíče kanálů – soubor jen pro vlastníka.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(data)
    print(f"Export uložen: {path}", file=sys.stderr)
    return 0


def _cmd_token(cfg, args) -> int:
    from .auth import TokenService
    from .db import Database
    from .db.repo import Repository

    setup_logging("WARNING")
    db = Database(cfg.database)
    if cfg.database.auto_migrate:
        db.migrate_sync()

    async def _run() -> int:
        await db.connect()
        try:
            repo = Repository(db.engine)
            if args.token_command == "create":
                existing = [t for t in await repo.list_tokens() if t["name"] == args.name]
                if existing and existing[0]["revoked_at"] is None:
                    print(f"Token s názvem {args.name!r} už existuje.", file=sys.stderr)
                    return 1
                if existing:  # zneplatněný token stejného jména nahradíme
                    await repo.delete_token(args.name)
                token = await TokenService(repo).create(args.name, args.scope)
                print(f"Token {args.name!r} ({args.scope}) vytvořen. Uložte si ho, znovu se nezobrazí:\n")
                print(f"  {token}\n")
                return 0
            if args.token_command == "list":
                rows = await repo.list_tokens()
                if not rows:
                    print("Žádné tokeny.")
                for t in rows:
                    state = "zneplatněn" if t["revoked_at"] else "aktivní"
                    used = t["last_used_at"].strftime("%Y-%m-%d %H:%M") if t["last_used_at"] else "nikdy"
                    print(f"  {t['name']:<24} {t['scopes']:<6} {state:<11} naposledy použit: {used}")
                return 0
            if await repo.revoke_token(args.name):
                print(f"Token {args.name!r} zneplatněn.")
                return 0
            print(f"Aktivní token {args.name!r} nenalezen.", file=sys.stderr)
            return 1
        finally:
            await db.close()

    return asyncio.run(_run())


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        cfg, path = load_config(args.config)
    except ConfigError as exc:
        print(f"Chyba: {exc}", file=sys.stderr)
        return 2

    if args.command == "check-config":
        return _cmd_check_config(cfg, path)
    if args.command in ("db", "token", "export"):
        try:
            if args.command == "token":
                return _cmd_token(cfg, args)
            if args.command == "export":
                return _cmd_export(cfg, args.target)
            return _cmd_db(cfg, args.db_command, args)
        except Exception as exc:  # noqa: BLE001 – srozumitelná chyba místo tracebacku
            print(f"Chyba databáze: {exc}", file=sys.stderr)
            return 1
    return _cmd_run(cfg, path)
