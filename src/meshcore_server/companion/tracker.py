"""Sledování uzlů: pravidelné dotazy na stav a telemetrii vybraných uzlů.

Repeater a room server: přihlášení (uložené heslo, jinak host) a dotaz na stav – baterie,
uptime, šum, airtime, počty paketů. Klient a senzor: telemetrie (Cayenne LPP), pokud ji uzel
povolí. Každý dotaz je paket v síti, proto se uzly berou postupně a v pevném intervalu;
po neúspěchu se interval prodlužuje (až na tracking.max_backoff_minutes).

Hodnoty se ukládají do node_samples pro grafy (tracking.history_days) a klientům chodí
událost node.tracked.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Optional

from .. import serialize
from ..db.repo import Repository
from ..db.types import utcnow
from .actions import LOGIN_TYPES, ActionError
from .ingest import battery_percent
from .manager import CompanionUnavailable

if TYPE_CHECKING:
    from ..config import TrackingConfig
    from ..events import EventBus
    from .actions import DeviceActions
    from .manager import CompanionManager

logger = logging.getLogger("meshcore_server.tracker")

CHECK_INTERVAL = 20.0   # s – jak často hledat uzly, na které je čas se zeptat
PAUSE_BETWEEN = 5.0     # s mezi dotazy na dva uzly
STATUS_FIELDS = (
    "bat", "tx_queue_len", "noise_floor", "last_rssi", "last_snr", "nb_recv", "nb_sent", "airtime", "rx_airtime",
    "uptime", "sent_flood", "sent_direct", "recv_flood", "recv_direct", "full_evts", "direct_dups", "flood_dups",
    "recv_errors",
)


def flatten_telemetry(lpp: Any) -> dict:
    """[{channel, type, value}] -> {"ch1_voltage": 4.1, "ch2_temperature": 21.5, "ch3_gps_latitude": …}"""
    out: dict = {}
    if not isinstance(lpp, list):
        return out
    for item in lpp:
        if not isinstance(item, dict):
            continue
        ch = item.get("channel")
        kind = re.sub(r"[^a-z0-9]+", "_", str(item.get("type", "value")).lower()).strip("_") or "value"
        value = item.get("value")
        base = f"ch{ch}_{kind}"
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            out[base] = value
        elif isinstance(value, dict):
            for k, v in value.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    out[f"{base}_{k}"] = v
    return out


def sample_from(status: Optional[dict], telemetry: Any) -> dict:
    """Ploché hodnoty pro historii. Baterie: bat (mV) a battery_percent."""
    data: dict = {}
    for k in STATUS_FIELDS:
        v = (status or {}).get(k)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            data[k] = v
    data.update(flatten_telemetry(telemetry))
    if "bat" not in data and isinstance(data.get("ch1_voltage"), (int, float)):
        data["bat"] = round(data["ch1_voltage"] * 1000)  # telemetrie: kanál 1 = napětí baterie uzlu
    if data.get("bat"):
        pct = battery_percent(int(data["bat"]))
        if pct is not None:
            data["battery_percent"] = pct
    return data


class NodeTracker:
    def __init__(self, repo: Repository, manager: "CompanionManager", actions: "DeviceActions",
                 bus: Optional["EventBus"], cfg: "TrackingConfig") -> None:
        self.repo = repo
        self.manager = manager
        self.actions = actions
        self.bus = bus
        self.cfg = cfg
        self._wake = asyncio.Event()

    @property
    def interval(self) -> timedelta:
        return timedelta(minutes=self.cfg.interval_minutes)

    def wake(self) -> None:
        self._wake.set()

    async def run(self, stop: asyncio.Event) -> None:
        if not self.cfg.enabled:
            logger.info("Sledování uzlů je vypnuté (tracking.enabled)")
            return
        while not stop.is_set():
            self._wake.clear()
            try:
                if self.manager.connected.is_set():
                    for key in await self.repo.due_tracked(utcnow()):
                        if stop.is_set() or not self.manager.connected.is_set():
                            break
                        await self.poll(key)
                        await asyncio.sleep(PAUSE_BETWEEN)
            except Exception:  # noqa: BLE001
                logger.exception("Sledování uzlů selhalo")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=CHECK_INTERVAL)
            except asyncio.TimeoutError:
                pass

    async def poll(self, key: str) -> None:
        row = await self.repo.get_tracked(key)
        if row is None:
            return
        contact = await self.repo.get_contact(key)
        name = (contact or {}).get("name") or key[:12]
        now = utcnow()
        status = telemetry = None
        try:
            if contact is None:
                raise ActionError("kontakt v databázi neexistuje")
            if contact.get("type") in LOGIN_TYPES:
                await self._ensure_login(key)
                status = (await self.actions.fetch_status(key)).get("status")
            else:
                telemetry = (await self.actions.fetch_telemetry(key)).get("telemetry")
        except CompanionUnavailable:
            # companion odpojený – zkusit po připojení, neúspěch uzlu nepočítat
            await self.repo.update_tracked(key, next_poll_at=now + timedelta(minutes=1))
            return
        except (ActionError, Exception) as exc:  # noqa: BLE001
            if contact is not None and contact.get("type") in LOGIN_TYPES:
                # uzel mohl mezitím restartovat a přihlášení zapomenout – příště se přihlásit znovu
                await self.repo.save_login(key, logged_in=False)
            failures = (row.get("failures") or 0) + 1
            wait = min(self.interval * (2 ** (failures - 1)), timedelta(minutes=self.cfg.max_backoff_minutes))
            await self.repo.update_tracked(
                key, last_poll_at=now, failures=failures, last_error=str(exc)[:500],
                **await self._next(key, row, now + wait),
            )
            logger.info("Sledování %s: %s (další pokus za %d min)", name, exc, wait.total_seconds() // 60)
            await self._publish(key)
            return

        values: dict = {"last_poll_at": now, "last_ok_at": now, "failures": 0, "last_error": None,
                        **await self._next(key, row, now + self.interval)}
        if status is not None:
            values["last_status"] = status
        if telemetry is not None:
            values["last_telemetry"] = telemetry
        await self.repo.update_tracked(key, **values)
        data = sample_from(status, telemetry)
        if data:
            await self.repo.add_node_sample(key, now, data)
        logger.info("Sledování %s: ok", name)
        await self._publish(key)

    async def _next(self, key: str, before: dict, when) -> dict:
        """Termín dalšího dotazu – pokud mezitím někdo požádal o dotaz hned (/poll), nepřepsat ho."""
        current = await self.repo.get_tracked(key)
        if current is not None and current.get("next_poll_at") != before.get("next_poll_at"):
            return {}
        return {"next_poll_at": when}

    async def _ensure_login(self, key: str) -> None:
        """Stav vrací uzel jen přihlášenému (stačí host). Uložené heslo, jinak prázdné."""
        login = await self.repo.get_login(key) or {}
        if login.get("logged_in"):
            return
        await self.actions.login_quiet(key, login.get("password") or "")

    async def _publish(self, key: str) -> None:
        row = await self.repo.get_tracked(key)
        if row and self.bus is not None:
            await self.bus.publish("node.tracked", serialize.tracked(row, self.interval))
