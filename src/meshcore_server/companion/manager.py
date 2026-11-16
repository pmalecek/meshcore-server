"""Správa spojení s companionem.

Drží jediné spojení, po připojení provede úvodní synchronizaci, hlídá spojení
keepalivem a při výpadku se znovu připojuje s rostoucí prodlevou.

Auto-reconnect knihovny meshcore nepoužíváme (má jen pár pokusů s pevnou
prodlevou); celý cyklus připojení řídíme sami, takže server se ke companionu
vrátí i po delším výpadku WiFi.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from meshcore import EventType, MeshCore

from ..config import CompanionConfig
from .ingest import DeviceMismatchError

logger = logging.getLogger("meshcore_server.companion")

EventHandler = Callable[[Any], Awaitable[None]]
# hook(mc, status, contacts: dict, channels: list[dict]) – volá se po synchronizaci
ConnectHook = Callable[..., Awaitable[None]]


class CompanionUnavailable(Exception):
    """Companion není připojený – příkaz nelze provést."""


def is_timeout(res) -> bool:
    """Companion na příkaz neodpověděl.

    Knihovna hlásí vypršení jako ERROR s reason "timeout" nebo
    "no_event_received"; skutečná chyba firmwaru má error_code.
    """
    if res is None:
        return True
    if res.type != EventType.ERROR:
        return False
    payload = res.payload or {}
    return "error_code" not in payload and payload.get("reason") in ("timeout", "no_event_received")


class ConnectionState(str, enum.Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    SYNCING = "syncing"
    CONNECTED = "connected"
    STOPPED = "stopped"


@dataclass
class CompanionStatus:
    """Aktuální stav – později ho bude vracet /status a /server."""

    state: ConnectionState = ConnectionState.DISCONNECTED
    connected_since: Optional[float] = None
    reconnects: int = 0
    last_error: Optional[str] = None
    self_info: dict = field(default_factory=dict)
    device_info: dict = field(default_factory=dict)
    contacts_count: int = 0
    default_scope: Optional[str] = None  # výchozí region (flood scope) zařízení
    last_rx_at: Optional[float] = None   # poslední data od companionu
    last_tx_at: Optional[float] = None   # poslední příkaz do companionu
    last_command_ms: Optional[float] = None  # jak dlouho trval poslední příkaz
    commands_total: int = 0
    channels: list[dict] = field(default_factory=list)
    battery: Optional[dict] = None  # poslední odpověď na get_bat (napětí, úložiště), viz battery_info()


class CompanionManager:
    def __init__(self, cfg: CompanionConfig) -> None:
        self.cfg = cfg
        self.status = CompanionStatus()
        self.mc: Optional[MeshCore] = None
        self._handlers: list[EventHandler] = []
        self._connect_hooks: list[ConnectHook] = []
        self._state_listeners: list[Callable[[CompanionStatus], Awaitable[None]]] = []
        self._lost = asyncio.Event()
        self._ever_connected = False
        # Knihovna meshcore příkazy neřadí: kdyby běžely dva naráz, mohly by si
        # prohodit odpovědi. Všechny příkazy proto jdou přes tento zámek.
        self.lock = asyncio.Lock()
        self.connected = asyncio.Event()   # nastaveno, když je companion připojen a synchronizován
        self._fetch_task: Optional[asyncio.Task] = None
        self._fetch_again = False
        self._waiters: list[tuple[set, Callable, asyncio.Future]] = []

    # ------------------------------------------------------------------ API

    def add_event_handler(self, handler: EventHandler) -> None:
        """Handler dostane každou událost z companionu (objekt meshcore Event)."""
        self._handlers.append(handler)

    def add_connect_hook(self, hook: ConnectHook) -> None:
        """Hook se volá po úvodní synchronizaci, před vyzvedáváním zpráv.

        Výjimka z hooku spojení ukončí (např. připojen jiný companion).
        """
        self._connect_hooks.append(hook)

    async def call(self, func):
        """Provede příkaz pod zámkem: await manager.call(lambda mc: mc.commands.get_bat()).

        Vyhodí CompanionUnavailable, když companion není připojený.
        """
        async with self.lock:
            mc = self.mc
            if mc is None or not self.connected.is_set():
                raise CompanionUnavailable("companion není připojen")
            started = time.monotonic()
            self.status.last_tx_at = time.time()
            try:
                return await func(mc)
            finally:
                self.status.last_command_ms = round((time.monotonic() - started) * 1000, 1)
                self.status.commands_total += 1

    async def wait_for(self, types, match: Callable[[Any], bool], timeout: float):
        """Počká na událost daného typu (bez držení zámku). Vrátí událost, nebo None po timeoutu.

        Na rozdíl od call() neblokuje ostatní příkazy – vhodné pro odpovědi
        vzdálených uzlů (přihlášení, stav), které trvají sekundy.
        """
        future = asyncio.get_running_loop().create_future()
        waiter = (set(types), match, future)
        self._waiters.append(waiter)
        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            if waiter in self._waiters:
                self._waiters.remove(waiter)

    async def resync(self) -> None:
        """Znovu načte vše z companionu (info, kontakty, kanály) a uloží přes hooky."""
        async with self.lock:
            mc = self.mc
            if mc is None or not self.connected.is_set():
                raise CompanionUnavailable("companion není připojen")
            contacts, channels = await self._initial_sync(mc)
            for hook in self._connect_hooks:
                await hook(mc, self.status, contacts, channels)
        self._schedule_fetch()

    def add_state_listener(self, listener: Callable[[CompanionStatus], Awaitable[None]]) -> None:
        """Listener dostane stav při každé změně (connecting, connected, …)."""
        self._state_listeners.append(listener)

    def _set_state(self, state: ConnectionState) -> None:
        if self.status.state == state:
            return
        self.status.state = state
        for listener in self._state_listeners:
            asyncio.create_task(_safe(listener(self.status)))

    async def run(self, stop: asyncio.Event) -> None:
        """Hlavní smyčka: připoj → synchronizuj → čekej na výpadek/stop → znovu."""
        delay = self.cfg.reconnect.initial_delay
        while not stop.is_set():
            self._set_state(ConnectionState.CONNECTING)
            logger.info("Připojuji se ke companionu (%s)…", self.cfg.describe())
            mc = await self._connect()

            if mc is None:
                self._set_state(ConnectionState.DISCONNECTED)
                logger.warning(
                    "Připojení se nezdařilo (%s), další pokus za %.0f s", self.status.last_error, delay
                )
                if await _wait_or_stop(stop, delay):
                    break
                delay = min(delay * 2, self.cfg.reconnect.max_delay)
                continue

            delay = self.cfg.reconnect.initial_delay
            if self._ever_connected:
                self.status.reconnects += 1
            self._ever_connected = True

            retry_delay = delay
            try:
                await self._session(mc, stop)
            except DeviceMismatchError as exc:
                logger.error("%s", exc)
                self.status.last_error = "připojen jiný companion než v databázi"
                retry_delay = self.cfg.reconnect.max_delay
            except Exception:  # noqa: BLE001 – spojení nesmí shodit celý server
                logger.exception("Neočekávaná chyba během spojení s companionem")
                self.status.last_error = "neočekávaná chyba, viz log"
            finally:
                await self._close(mc)

            if not stop.is_set():
                self._set_state(ConnectionState.DISCONNECTED)
                logger.warning(
                    "Spojení s companionem ukončeno (%s), znovu za %.0f s", self.status.last_error, retry_delay
                )
                if await _wait_or_stop(stop, retry_delay):
                    break

        self._set_state(ConnectionState.STOPPED)
        logger.info("Správce companionu ukončen")

    # ------------------------------------------------------------- interní

    async def _connect(self) -> Optional[MeshCore]:
        c = self.cfg
        try:
            if c.transport == "tcp":
                mc = await MeshCore.create_tcp(
                    c.tcp.host, c.tcp.port, default_timeout=c.command_timeout
                )
            elif c.transport == "serial":
                mc = await MeshCore.create_serial(
                    c.serial.port, c.serial.baudrate, default_timeout=c.command_timeout
                )
            else:
                mc = await MeshCore.create_ble(
                    c.ble.address, pin=c.ble.pin, default_timeout=c.command_timeout
                )
        except Exception as exc:  # noqa: BLE001 – OSError, timeouty, chyby BLE…
            self.status.last_error = f"{type(exc).__name__}: {exc}"
            logger.debug("Chyba při připojování", exc_info=True)
            return None

        if mc is None:
            self.status.last_error = "companion neodpověděl"
        return mc

    async def _session(self, mc: MeshCore, stop: asyncio.Event) -> None:
        self.mc = mc
        self._lost.clear()

        mc.subscribe(EventType.DISCONNECTED, self._on_disconnected)
        mc.subscribe(EventType.MESSAGES_WAITING, self._on_messages_waiting)
        mc.subscribe(None, self._dispatch)

        self._set_state(ConnectionState.SYNCING)
        async with self.lock:
            contacts, channels = await self._initial_sync(mc)
            for hook in self._connect_hooks:
                await hook(mc, self.status, contacts, channels)
        if self.cfg.decrypt_channel_logs:
            # Knihovna pak u kanálových zpráv doplní trasu, SNR a RSSI z RX logu.
            mc.set_decrypt_channel_logs(True)

        self.status.connected_since = time.time()
        self.status.last_error = None
        self.connected.set()
        self._set_state(ConnectionState.CONNECTED)
        # Zprávy, které companion nasbíral, než jsme se připojili.
        self._schedule_fetch()
        logger.info("Companion připojen a synchronizován, čekám na události")

        tasks = [
            asyncio.create_task(stop.wait(), name="stop"),
            asyncio.create_task(self._lost.wait(), name="lost"),
        ]
        if self.cfg.keepalive_interval > 0:
            tasks.append(asyncio.create_task(self._keepalive(mc), name="keepalive"))
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            self.connected.clear()
            for task in tasks:
                task.cancel()
            if self._fetch_task is not None:
                self._fetch_task.cancel()
                tasks.append(self._fetch_task)
                self._fetch_task = None
            await asyncio.gather(*tasks, return_exceptions=True)
            self.status.connected_since = None

    async def _initial_sync(self, mc: MeshCore) -> tuple[dict, list[dict]]:
        # SELF_INFO knihovna načetla už při připojení (APP_START).
        self.status.self_info = dict(mc.self_info or {})
        name = self.status.self_info.get("name", "?")
        pubkey = self.status.self_info.get("public_key", "")
        logger.info("Node: %s (public key %s…)", name, pubkey[:16])

        res = await mc.commands.send_device_query()
        if res.type == EventType.DEVICE_INFO:
            self.status.device_info = dict(res.payload)
            d = res.payload
            logger.info(
                "Firmware: %s (%s), model: %s, max. kontaktů: %s, max. kanálů: %s",
                d.get("ver", "?"), d.get("fw_build", "?"), d.get("model", "?"),
                d.get("max_contacts", "?"), d.get("max_channels", "?"),
            )
        else:
            logger.warning("Dotaz na info o zařízení selhal: %s", res.payload)

        if self.cfg.set_time_on_connect:
            res = await mc.commands.set_time(int(time.time()))
            if res.type == EventType.ERROR:
                logger.warning("Nastavení času companionu selhalo: %s", res.payload)
            else:
                logger.info("Čas companionu nastaven podle serveru")

        res = await mc.commands.get_contacts()
        if res.type == EventType.CONTACTS and isinstance(res.payload, dict):
            contacts = dict(res.payload)
            self.status.contacts_count = len(contacts)
            logger.info("Kontakty v companionu: %d", self.status.contacts_count)
        else:
            # Bez seznamu kontaktů nemá smysl pokračovat – DB by si myslela,
            # že companion žádné nemá.
            raise RuntimeError(f"načtení kontaktů selhalo: {getattr(res, 'payload', res)}")

        res = await mc.commands.get_default_flood_scope()
        if res.type == EventType.DEFAULT_FLOOD_SCOPE:
            self.status.default_scope = (res.payload or {}).get("scope_name") or None
            logger.info("Výchozí region: %s", self.status.default_scope or "žádný")
        else:
            self.status.default_scope = None  # starší firmware výchozí region nezná

        channels = await self._read_channels(mc)
        self.status.channels = [{"idx": c["idx"], "name": c["name"]} for c in channels]
        if channels:
            names = ", ".join(f"{c['idx']}:{c['name']}" for c in channels)
            logger.info("Kanály (%d): %s", len(channels), names)
        else:
            logger.info("Žádné nastavené kanály")
        return contacts, channels

    async def _read_channels(self, mc: MeshCore) -> list[dict]:
        max_channels = int(self.status.device_info.get("max_channels") or 8)
        channels = []
        for idx in range(max_channels):
            res = await mc.commands.get_channel(idx)
            if res.type != EventType.CHANNEL_INFO:
                # Starší firmware vrací chybu za posledním slotem.
                break
            info = res.payload or {}
            if info.get("channel_name"):
                secret = info.get("channel_secret")
                channels.append({
                    "idx": info.get("channel_idx", idx),
                    "name": info["channel_name"],
                    "secret": secret.hex() if isinstance(secret, (bytes, bytearray)) else secret,
                })
        return channels

    # ------------------------------------------------- vyzvedávání zpráv

    def _schedule_fetch(self) -> None:
        """Vyzvedne čekající zprávy; při souběžné žádosti proběhne ještě jedno kolo."""
        if self._fetch_task is not None and not self._fetch_task.done():
            self._fetch_again = True
            return
        self._fetch_task = asyncio.create_task(self._fetch_messages())

    async def _fetch_messages(self) -> None:
        while True:
            self._fetch_again = False
            for _ in range(500):  # pojistka proti nekonečné smyčce
                try:
                    res = await self.call(lambda mc: mc.commands.get_msg())
                except CompanionUnavailable:
                    return
                if res is None or res.type in (EventType.NO_MORE_MSGS, EventType.ERROR):
                    break
            if not self._fetch_again:
                return

    async def _on_messages_waiting(self, _event) -> None:
        self._schedule_fetch()

    # --------------------------------------------------------- keepalive

    async def _keepalive(self, mc: MeshCore) -> None:
        failures = 0
        while True:
            await asyncio.sleep(self.cfg.keepalive_interval)
            async with self.lock:
                res = await mc.commands.get_bat()
            timed_out = is_timeout(res)
            if timed_out:
                failures += 1
                logger.warning(
                    "Companion neodpověděl na keepalive (%d/%d)", failures, self.cfg.keepalive_failures
                )
                if failures >= self.cfg.keepalive_failures:
                    self.status.last_error = "companion přestal odpovídat"
                    return
            else:
                failures = 0

    async def _on_disconnected(self, event) -> None:
        self.status.last_error = f"odpojeno: {(event.payload or {}).get('reason', '?')}"
        self._lost.set()

    async def _dispatch(self, event) -> None:
        if event.type not in (EventType.CONNECTED, EventType.DISCONNECTED):
            self.status.last_rx_at = time.time()
        for types, match, future in list(self._waiters):
            if event.type in types and not future.done():
                try:
                    if match(event):
                        future.set_result(event)
                except Exception:  # noqa: BLE001
                    logger.debug("Chyba v podmínce čekání na událost", exc_info=True)
        for handler in self._handlers:
            try:
                await handler(event)
            except Exception:  # noqa: BLE001
                logger.exception("Chyba v obsluze události %s", event.type)

    async def _close(self, mc: MeshCore) -> None:
        self.mc = None
        try:
            await asyncio.wait_for(mc.disconnect(), timeout=5)
        except Exception:  # noqa: BLE001
            logger.debug("Chyba při odpojování", exc_info=True)


async def _safe(coro) -> None:
    try:
        await coro
    except Exception:  # noqa: BLE001
        logger.exception("Chyba v listeneru stavu companionu")


async def _wait_or_stop(stop: asyncio.Event, seconds: float) -> bool:
    """Počká zadanou dobu; vrátí True, pokud mezitím přišel požadavek na ukončení."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return False
