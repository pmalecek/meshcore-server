"""Sestavení a běh serveru."""

from __future__ import annotations

import asyncio
import logging
import signal
from datetime import timedelta
from pathlib import Path
from typing import Optional

from . import __version__
from .auth import TokenService
from .companion import CompanionManager
from .companion.event_log import EventLogger
from .companion.ingest import Ingest
from .companion.actions import DeviceActions
from .companion.queue import CommandQueue
from .companion.tracker import NodeTracker
from .config import Config
from .db import Database
from .db.repo import Repository
from .db.types import utcnow
from .events import EventBus
from .hops import HopResolver

logger = logging.getLogger("meshcore_server")

MAINTENANCE_INTERVAL = 3600  # s


class Application:
    def __init__(self, cfg: Config, config_path: Optional[Path] = None) -> None:
        self.cfg = cfg
        self.config_path = config_path
        self.stop = asyncio.Event()
        self.db = Database(cfg.database)
        self.companion = CompanionManager(cfg.companion)
        if cfg.logging.log_events:
            self.companion.add_event_handler(EventLogger(cfg.logging.log_noisy_events))

    def request_stop(self) -> None:
        if not self.stop.is_set():
            logger.info("Ukončuji server…")
            self.stop.set()

    async def run(self) -> None:
        logger.info("meshcore-server %s", __version__)
        logger.info(
            "Konfigurace: %s",
            self.config_path if self.config_path else "bez souboru (výchozí hodnoty + proměnné prostředí)",
        )
        self._install_signal_handlers()

        try:
            if self.cfg.database.auto_migrate:
                logger.info("Kontroluji schéma databáze…")
                await self.db.migrate()
            await self.db.connect()
        except Exception as exc:  # noqa: BLE001
            reason = str(getattr(exc, "orig", exc))
            logger.error("Databáze %s není dostupná: %s", self.db.describe(), reason)
            if self.db.is_sqlite and ("readonly" in reason or "unable to open" in reason):
                import os

                folder = os.path.dirname(os.path.abspath(self.cfg.database.sqlite_path))
                logger.error(
                    "Server (uživatel UID %s) nemůže zapisovat do %s. Adresář i soubory v něm mu musí patřit, "
                    "např.: chown -R %s %s (v Dockeru na hostiteli pro připojený adresář s daty).",
                    os.getuid(), folder, os.getuid(), folder,
                )
            raise SystemExit(1) from None
        try:
            await self._run_services()
        finally:
            await self.db.close()

    async def _run_services(self) -> None:
        from .api import build_server
        from .api.context import ServerContext

        repo = Repository(self.db.engine)
        bus = EventBus(repo)
        await bus.init()
        tokens = TokenService(repo)
        hops = HopResolver(repo)

        ingest = Ingest(repo, self.cfg.companion, bus, hops)
        ingest.attach(self.companion)
        actions = DeviceActions(repo, self.companion, bus, hops)
        self.companion.add_state_listener(actions.on_state)
        queue = CommandQueue(repo, self.companion, self.cfg.companion, bus, hops, actions)
        tracker = NodeTracker(repo, self.companion, actions, bus, self.cfg.tracking)

        if not await repo.list_tokens():
            logger.warning(
                "Neexistuje žádný API token – vytvořte ho: meshcore-server token create <název> --scope admin"
            )

        ctx = ServerContext(cfg=self.cfg, db=self.db, repo=repo, bus=bus, tokens=tokens, companion=self.companion,
                            hops=hops, queue=queue, actions=actions, tracker=tracker)
        api = build_server(ctx)
        logger.info("API poslouchá na http://%s:%d", self.cfg.server.host, self.cfg.server.port)

        tasks = {
            asyncio.create_task(self.companion.run(self.stop), name="companion"),
            asyncio.create_task(self._serve_api(api), name="api"),
            asyncio.create_task(self._maintenance(repo), name="maintenance"),
            asyncio.create_task(queue.run(self.stop), name="queue"),
            asyncio.create_task(tracker.run(self.stop), name="tracker"),
        }
        await self.stop.wait()
        api.should_exit = True
        done, pending = await asyncio.wait(tasks, timeout=10)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _serve_api(self, api) -> None:
        try:
            await api.serve()
        except (SystemExit, Exception):  # uvicorn při chybě startu (obsazený port) volá sys.exit
            logger.error("API se nepodařilo spustit (port %d obsazený?)", self.cfg.server.port)
        if not self.stop.is_set():
            if not api.started:
                logger.error("API neběží, ukončuji server")
            self.request_stop()

    async def _maintenance(self, repo: Repository) -> None:
        """Pravidelný úklid: staré události pro WebSocket a pročišťování podle retence."""
        keep = timedelta(days=self.cfg.database.events_retention_days)
        r = self.cfg.database.retention
        while not self.stop.is_set():
            try:
                removed = await repo.prune_events(utcnow() - keep)
                if removed:
                    logger.info("Úklid: smazáno %d starých událostí", removed)
                samples = await repo.prune_node_samples(utcnow() - timedelta(days=self.cfg.tracking.history_days))
                if samples:
                    logger.info("Úklid: smazáno %d starých hodnot sledovaných uzlů", samples)
                purged = await repo.purge(utcnow(), r.messages_days, r.adverts_days, r.commands_days)
                if any(purged.values()):
                    logger.info("Pročištění: %s", ", ".join(f"{k} {v}" for k, v in purged.items() if v))
            except Exception:  # noqa: BLE001
                logger.exception("Úklid databáze selhal")
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=MAINTENANCE_INTERVAL)
            except asyncio.TimeoutError:
                pass

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop)
            except (NotImplementedError, RuntimeError):
                # Windows – Ctrl+C se projeví jako KeyboardInterrupt v cli.py.
                pass
