"""Ukládání dat z companionu do databáze.

on_connected() se volá po úvodní synchronizaci (ještě před vyzvedáváním zpráv),
handle() dostává každou událost z companionu.
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone
import logging
from typing import TYPE_CHECKING, Any, Optional

from meshcore import EventType

from .. import serialize
from ..db.repo import Repository
from ..db.types import utcnow

if TYPE_CHECKING:
    from ..events import EventBus
    from ..config import CompanionConfig
    from .manager import CompanionManager

logger = logging.getLogger("meshcore_server.ingest")

# Klíče SELF_INFO, které ukládáme do device_config (identita jde do tabulky device).
_SELF_INFO_SKIP = {"public_key", "name", "adv_type"}

# Jak dlouho čekat po advertu/změně trasy, než se načtou změněné kontakty
# (adverty chodí ve vlnách, ať se companion nedotazuje po každém zvlášť).
CONTACT_REFRESH_DELAY = 3.0


class DeviceMismatchError(Exception):
    """K serveru se připojil jiný companion, než jaký je uložený v DB."""


def _dedup_hash(*parts: Any) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()


def _split_sender(text: str) -> tuple[Optional[str], str]:
    """Kanálové zprávy mají tvar „Jméno: text“."""
    name, sep, body = text.partition(": ")
    if sep and 0 < len(name) <= 64 and "\n" not in name:
        return name, body
    return None, text


def _path_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.hex()
    text = str(value)
    return text[:255] if text else None


class Ingest:
    def __init__(
        self, repo: Repository, cfg: "CompanionConfig", bus: Optional["EventBus"] = None, hops=None
    ) -> None:
        self.repo = repo
        self.cfg = cfg
        self.bus = bus
        self.hops = hops  # HopResolver – doplní jména repeaterů do trasy
        self.manager: Optional["CompanionManager"] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._contacts_lastmod = 0
        # Public key companionu, který prošel kontrolou; jen jeho data se ukládají.
        self._approved_key: Optional[str] = None

    def attach(self, manager: "CompanionManager") -> None:
        self.manager = manager
        manager.add_event_handler(self.handle)
        manager.add_connect_hook(self.on_connected)
        manager.add_state_listener(self.on_state)

    async def _publish(self, type_: str, data: Any) -> None:
        if self.bus is not None:
            await self.bus.publish(type_, data)

    async def on_state(self, status) -> None:
        await self._publish("device.connection", {
            "state": status.state.value,
            "connected_since": datetime.fromtimestamp(status.connected_since, tz=timezone.utc)
            if status.connected_since else None,
            "last_error": status.last_error,
            "reconnects": status.reconnects,
        })

    async def _publish_contacts(self, keys) -> None:
        if self.hops is not None:
            self.hops.invalidate()
        if self.bus is None:
            return
        for row in await self.repo.get_contacts_by_keys(keys):
            await self._publish("contact.updated", serialize.contact(row))

    # ---------------------------------------------------------- připojení

    async def on_connected(self, mc, status, contacts: dict, channels: list[dict]) -> None:
        info = status.self_info
        dev = status.device_info
        pubkey = info.get("public_key")
        if not pubkey:
            raise RuntimeError("companion nevrátil public key")

        self._approved_key = None
        stored = await self.repo.get_device()
        changed = stored is not None and stored["public_key"] != pubkey
        if changed and not self.cfg.allow_device_change:
            raise DeviceMismatchError(
                f"Připojený companion ({info.get('name')}, {pubkey[:16]}…) není ten uložený "
                f"v databázi ({stored.get('name')}, {stored['public_key'][:16]}…). Pokud je změna "
                "záměrná, nastavte companion.allow_device_change: true a restartujte server."
            )
        self._approved_key = pubkey
        if changed:
            logger.warning("Companion se změnil na %s (%s…), ukládám nový", info.get("name"), pubkey[:16])

        await self.repo.save_device(
            {
                "public_key": pubkey,
                "name": info.get("name"),
                "adv_type": info.get("adv_type"),
                "model": dev.get("model"),
                "firmware_ver": dev.get("ver"),
                "firmware_build": dev.get("fw_build"),
                "protocol_ver": dev.get("fw ver"),
                "max_contacts": dev.get("max_contacts"),
                "max_channels": dev.get("max_channels"),
                "capabilities": {
                    "protocol_ver": dev.get("fw ver"),
                    "repeat": dev.get("repeat"),
                    "path_hash_mode": dev.get("path_hash_mode"),
                },
                "transport": self.cfg.transport,
                "transport_config": self._transport_config(),
            },
            replace_identity=changed,
        )
        config = {k: v for k, v in info.items() if k not in _SELF_INFO_SKIP}
        config["default_flood_scope"] = status.default_scope
        await self.repo.save_device_config(config)

        if contacts:
            await self.repo.upsert_contacts(contacts.values(), on_device=True)
        off = await self.repo.mark_contacts_off_device(contacts.keys())
        await self.repo.replace_channels(channels)
        self._contacts_lastmod = max((c.get("lastmod", 0) for c in contacts.values()), default=0)
        logger.info(
            "Uloženo do DB: %d kontaktů (%d mimo companion), %d kanálů", len(contacts), off, len(channels)
        )
        # Po úvodní synchronizaci klientům stačí vědět, že mají data načíst znovu.
        await self._publish("device.updated", serialize.device(
            await self.repo.get_device(), await self.repo.get_device_config()
        ))
        await self._publish("channels.updated", [serialize.channel(r) for r in await self.repo.list_channels()])
        if self.hops is not None:
            self.hops.invalidate()
        await self._publish("contacts.synced", {"on_device": len(contacts), "off_device_changed": off})

    def _transport_config(self) -> dict:
        c = self.cfg
        if c.transport == "tcp":
            return {"host": c.tcp.host, "port": c.tcp.port}
        if c.transport == "serial":
            return {"port": c.serial.port, "baudrate": c.serial.baudrate}
        return {"address": c.ble.address}

    # ------------------------------------------------------------ události

    def _accepting(self) -> bool:
        mc = self.manager.mc if self.manager else None
        return (
            mc is not None
            and self._approved_key is not None
            and (mc.self_info or {}).get("public_key") == self._approved_key
        )

    async def handle(self, event) -> None:
        if not self._accepting():
            return
        t = event.type
        p = event.payload if isinstance(event.payload, dict) else {}
        if t == EventType.CHANNEL_MSG_RECV:
            await self._channel_message(p)
        elif t == EventType.CONTACT_MSG_RECV:
            if p.get("txt_type") == 1:
                return  # odpověď na CLI příkaz – nepatří do zpráv, vrací ji API /cli
            await self._direct_message(p)
        elif t == EventType.CONTACTS:
            # úvodní i průběžné (lastmod) načtení kontaktů
            if isinstance(event.payload, dict) and event.payload:
                await self.repo.upsert_contacts(event.payload.values(), on_device=True)
            lastmod = (event.attributes or {}).get("lastmod")
            if lastmod:
                self._contacts_lastmod = lastmod
        elif t == EventType.NEW_CONTACT:
            # Režim ručního přidávání: kontakt z advertu, který v companionu není.
            await self.repo.upsert_contacts([p], on_device=False)
            await self._publish_contacts([p.get("public_key")])
            await self._advert(p.get("public_key"), p)
        elif t == EventType.ADVERTISEMENT:
            await self._advert(p.get("public_key"), None)
            self._schedule_contact_refresh()
        elif t == EventType.PATH_UPDATE:
            self._schedule_contact_refresh()
        elif t == EventType.CONTACT_DELETED:
            key = p.get("pubkey") or p.get("public_key")
            if key:
                await self.repo.set_contact_off_device(key)
                await self._publish_contacts([key])
        elif t == EventType.CONTACTS_FULL:
            logger.warning("Companion hlásí plnou paměť kontaktů")

    async def _channel_message(self, p: dict) -> None:
        text = p.get("text", "")
        sender, body = _split_sender(text)
        idx = p.get("channel_idx")
        sender_ts = p.get("sender_timestamp")
        row = {
            "kind": "channel",
            "direction": "in",
            "channel_idx": idx,
            "sender_name": sender,
            "text": body,
            "txt_type": p.get("txt_type"),
            "sender_ts": sender_ts,
            "received_at": utcnow(),
            "path_len": p.get("path_len"),
            "path": _path_str(p.get("path")),
            "snr": p.get("SNR"),
            "rssi": p.get("RSSI"),
            "status": "received",
            "attempts": 0,
            "dedup_hash": _dedup_hash("channel", idx, sender_ts, text),
        }
        msg_id = await self.repo.insert_message(row)
        if msg_id is None:
            logger.debug("Duplicitní zpráva v kanálu %s ignorována", idx)
        else:
            await self._publish_message(msg_id)

    async def _direct_message(self, p: dict) -> None:
        prefix = (p.get("pubkey_prefix") or "").lower()
        text = p.get("text", "")
        sender_ts = p.get("sender_timestamp")
        contact = await self._resolve_contact(prefix)
        now = utcnow()
        is_room = bool(contact) and contact.get("type") == 3
        sender_name = contact.get("name") if contact else None
        author = None
        if is_room and p.get("txt_type") == 2 and p.get("signature"):
            # Příspěvek z room serveru: skutečný autor je v „signature“ (prefix jeho klíče).
            author = str(p["signature"]).lower()[:8]
            author_contact = await self._resolve_contact(author)
            sender_name = (author_contact or {}).get("name") or author
        row = {
            "kind": "room" if is_room else "dm",
            "direction": "in",
            "contact_prefix": prefix or None,
            "contact_key": contact.get("public_key") if contact else None,
            "author_prefix": author,
            "sender_name": sender_name,
            "text": text,
            "txt_type": p.get("txt_type"),
            "sender_ts": sender_ts,
            "received_at": now,
            "path_len": p.get("path_len"),
            "path": _path_str(p.get("path")),
            "snr": p.get("SNR"),
            "rssi": p.get("RSSI"),
            "status": "received",
            "attempts": 0,
            "dedup_hash": (_dedup_hash("room", prefix, author, sender_ts, text) if is_room
                           else _dedup_hash("dm", prefix, sender_ts, text)),
        }
        msg_id = await self.repo.insert_message(row)
        if msg_id is None:
            logger.debug("Duplicitní DM od %s ignorována", prefix)
            return
        if contact:
            await self.repo.touch_contact_message(contact["public_key"], now)
        await self._publish_message(msg_id)

    async def _publish_message(self, msg_id: int) -> None:
        if self.bus is None:
            return
        row = await self.repo.get_message(msg_id)
        if row:
            await self._publish("message.received", await serialize.message_with_hops(row, self.hops))

    async def _resolve_contact(self, prefix: str) -> Optional[dict]:
        if not prefix:
            return None
        mc = self.manager.mc if self.manager else None
        if mc is not None:
            c = mc.get_contact_by_key_prefix(prefix)
            if c:
                return {"public_key": c["public_key"], "name": c.get("adv_name"), "type": c.get("type")}
        return await self.repo.find_contact_by_prefix(prefix)

    async def _advert(self, public_key: Optional[str], data: Optional[dict]) -> None:
        if not public_key:
            return
        if data is None:
            data = {}
            mc = self.manager.mc if self.manager else None
            known = (mc.contacts.get(public_key) if mc is not None else None) or {}
            if known:
                data = known
            else:
                stored = await self.repo.get_contact(public_key)
                if stored:
                    data = {"adv_name": stored["name"], "type": stored["type"],
                            "adv_lat": stored["lat"], "adv_lon": stored["lon"]}
        advert_id = await self.repo.insert_advert({
            "public_key": public_key,
            "name": (data.get("adv_name") or None),
            "type": data.get("type"),
            "lat": data.get("adv_lat") or None,
            "lon": data.get("adv_lon") or None,
            # Trasu a SNR samotného advertu companion v této notifikaci neposílá.
            "path_len": None,
            "path": None,
            "snr": None,
            "received_at": utcnow(),
        })
        if self.bus is not None:
            row = (await self.repo.list_adverts(before=advert_id + 1, limit=1) or [None])[0]
            if row:
                await self._publish("advert.received", serialize.advert(row))

    def _schedule_contact_refresh(self) -> None:
        if self._refresh_task is None or self._refresh_task.done():
            self._refresh_task = asyncio.create_task(self._refresh_contacts())

    async def _refresh_contacts(self) -> None:
        await asyncio.sleep(CONTACT_REFRESH_DELAY)
        if self.manager is None:
            return
        from .manager import CompanionUnavailable

        try:
            res = await self.manager.call(lambda mc: mc.commands.get_contacts(lastmod=self._contacts_lastmod))
        except CompanionUnavailable:
            return
        if res.type != EventType.CONTACTS or not isinstance(res.payload, dict):
            logger.debug("Průběžné načtení kontaktů selhalo: %s", getattr(res, "payload", res))
            return
        if res.payload:
            # Uloží i handle(); tady navíc klientům oznámíme, co se změnilo.
            await self.repo.upsert_contacts(res.payload.values(), on_device=True)
            await self._publish_contacts(res.payload.keys())
