"""Správa companionu: kontakty a kanály.

Na rozdíl od zpráv se tyto příkazy provádějí hned (pod zámkem spojení)
a API vrací výsledek přímo. Do tabulky commands se zapisují pro historii.
Když companion není připojený, akce vyhodí CompanionUnavailable (API → 503).
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Optional

from meshcore import EventType

from .. import serialize
from ..db.repo import Repository
from ..db.types import utcnow
from .manager import CompanionUnavailable, is_timeout

if TYPE_CHECKING:
    from ..events import EventBus
    from ..hops import HopResolver
    from .manager import CompanionManager

logger = logging.getLogger("meshcore_server.actions")


LOGIN_MIN_TIMEOUT = 8.0     # s
LOGIN_MAX_TIMEOUT = 60.0    # s
RELOGIN_DELAY = 3.0         # s mezi automatickými přihlášeními po reconnectu
LOGIN_TYPES = (2, 3)        # repeater, room server


class ActionError(Exception):
    """Companion akci odmítl nebo ji nelze provést."""


def _check(res, what: str) -> None:
    if is_timeout(res):
        raise CompanionUnavailable(f"{what}: companion neodpověděl")
    if res.type == EventType.ERROR:
        payload = res.payload or {}
        reason = payload.get("code_string") or payload.get("reason") or payload.get("error_code")
        raise ActionError(f"{what} selhalo: {reason}")


class DeviceActions:
    def __init__(
        self,
        repo: Repository,
        manager: "CompanionManager",
        bus: Optional["EventBus"] = None,
        hops: Optional["HopResolver"] = None,
    ) -> None:
        self.repo = repo
        self.manager = manager
        self.bus = bus
        self.hops = hops

    # ----------------------------------------------------------- pomocné

    async def _publish(self, type_: str, data: Any) -> None:
        if self.bus is not None:
            await self.bus.publish(type_, data)

    async def _publish_contact(self, key: str) -> None:
        if self.hops is not None:
            self.hops.invalidate()
        row = await self.repo.get_contact(key)
        if row:
            await self._publish("contact.updated", serialize.contact(row))

    async def _publish_channels(self) -> None:
        await self._publish("channels.updated", [serialize.channel(r) for r in await self.repo.list_channels()])

    async def _run(self, type_: str, payload: dict, token_id: Optional[int], coro):
        """Provede akci a zapíše ji do historie příkazů."""
        try:
            result = await coro
        except ActionError as exc:
            await self.repo.record_command(type_, payload, "failed", str(exc), token_id)
            raise
        await self.repo.record_command(type_, payload, "done", None, token_id)
        return result

    # ---------------------------------------------------------- kontakty

    async def ensure_on_device(self, key: str, token_id: Optional[int] = None) -> None:
        """Zajistí, že kontakt je v companionu (přidá ho, případně uvolní místo)."""
        mc = self.manager.mc
        if mc is not None and key in mc.contacts:
            return
        await self.add_to_device(key, token_id)

    async def add_to_device(self, key: str, token_id: Optional[int] = None) -> dict:
        return await self._run("contact_add", {"public_key": key}, token_id, self._add_to_device(key))

    async def _add_to_device(self, key: str) -> dict:
        row = await self.repo.get_contact(key)
        if row is None:
            raise ActionError("kontakt v databázi neexistuje")
        mc = self.manager.mc
        if mc is not None and key in mc.contacts:
            return serialize.contact(row)

        await self._make_room(exclude=key)

        # Kontakt přidáme s flood trasou; přímou trasu si companion najde sám.
        contact = {
            "public_key": key,
            "type": row.get("type") or 1,
            "flags": row.get("flags") or 0,
            "out_path": "",
            "out_path_len": -1,
            "out_path_hash_mode": 0,
            "adv_name": (row.get("name") or "")[:31],
            "last_advert": int(row["last_advert_at"].timestamp()) if row.get("last_advert_at") else 0,
            "adv_lat": row.get("lat") or 0.0,
            "adv_lon": row.get("lon") or 0.0,
        }

        async def do(mc):
            res = await mc.commands.add_contact(contact)
            _check(res, "přidání kontaktu")
            # Načíst kontakt zpět, ať knihovna i DB mají to, co companion opravdu uložil.
            res = await mc.commands.get_contact_by_key(bytes.fromhex(key))
            if res.type == EventType.NEXT_CONTACT and isinstance(res.payload, dict):
                mc.contacts[key] = res.payload
                return res.payload
            mc.contacts[key] = contact
            return contact

        stored = await self.manager.call(do)
        await self.repo.upsert_contacts([stored], on_device=True)
        logger.info("Kontakt %s přidán do companionu", row.get("name") or key[:12])
        await self._publish_contact(key)
        return serialize.contact(await self.repo.get_contact(key))

    async def _make_room(self, exclude: Optional[str] = None) -> None:
        device = await self.repo.get_device()
        limit = (device or {}).get("max_contacts")
        if not limit:
            return
        while await self.repo.count_on_device() >= limit:
            victim = await self.repo.eviction_candidate(exclude=exclude)
            if victim is None:
                raise ActionError("companion je plný a všechny kontakty v něm jsou oblíbené")
            logger.info(
                "Companion je plný (%d), odebírám nejdéle neaktivní kontakt %s",
                limit, victim.get("name") or victim["public_key"][:12],
            )
            await self._remove_from_device(victim["public_key"])

    async def remove_from_device(self, key: str, token_id: Optional[int] = None) -> dict:
        return await self._run("contact_remove", {"public_key": key}, token_id, self._remove_from_device(key))

    async def _remove_from_device(self, key: str) -> dict:
        async def do(mc):
            res = await mc.commands.remove_contact(key)
            # Kontakt, který v companionu není, firmware hlásí jako chybu – to nevadí.
            if res.type == EventType.ERROR and (res.payload or {}).get("error_code") == 2:
                return
            _check(res, "odebrání kontaktu")

        await self.manager.call(do)
        mc = self.manager.mc
        if mc is not None:
            mc.contacts.pop(key, None)
        await self.repo.update_contact_fields(key, on_device=False)
        await self._publish_contact(key)
        row = await self.repo.get_contact(key)
        return serialize.contact(row) if row else {"public_key": key}

    async def delete_contact(self, key: str, token_id: Optional[int] = None) -> None:
        async def go():
            row = await self.repo.get_contact(key)
            if row and row["on_device"]:
                await self._remove_from_device(key)
            await self.repo.delete_contact(key)
            if self.hops is not None:
                self.hops.invalidate()
            await self._publish("contact.deleted", {"public_key": key})

        await self._run("contact_delete", {"public_key": key}, token_id, go())

    async def reset_path(self, key: str, token_id: Optional[int] = None) -> dict:
        async def go():
            async def do(mc):
                if key not in mc.contacts:
                    raise ActionError("kontakt není v companionu")
                _check(await mc.commands.reset_path(key), "reset trasy")

            await self.manager.call(do)
            await self.repo.update_contact_fields(key, out_path=None, out_path_len=-1)
            await self._publish_contact(key)
            return serialize.contact(await self.repo.get_contact(key))

        return await self._run("contact_reset_path", {"public_key": key}, token_id, go())

    async def set_favorite(self, key: str, favorite: bool) -> dict:
        await self.repo.update_contact_fields(key, favorite=favorite)
        await self._publish_contact(key)
        return serialize.contact(await self.repo.get_contact(key))

    async def set_manual_add(self, manual: bool, token_id: Optional[int] = None) -> None:
        async def go():
            async def do(mc):
                _check(await mc.commands.set_manual_add_contacts(manual), "nastavení přidávání kontaktů")

            await self.manager.call(do)
            await self.repo.save_device_config({"manual_add_contacts": manual})

        await self._run("set_manual_add", {"manual_add_contacts": manual}, token_id, go())

    # ------------------------------------------------------------ kanály

    async def set_channel(
        self, idx: int, name: str, secret: Optional[str], region: Optional[str], token_id: Optional[int] = None
    ) -> dict:
        payload = {"idx": idx, "name": name, "region": region, "secret_given": secret is not None}
        return await self._run("channel_set", payload, token_id, self._set_channel(idx, name, secret, region))

    async def _set_channel(self, idx: int, name: str, secret: Optional[str], region: Optional[str]) -> dict:
        device = await self.repo.get_device()
        max_channels = (device or {}).get("max_channels") or 8
        if not 0 <= idx < max_channels:
            raise ActionError(f"číslo kanálu musí být 0–{max_channels - 1}")
        secret_bytes = bytes.fromhex(secret) if secret else None

        async def do(mc):
            _check(await mc.commands.set_channel(idx, name, secret_bytes), "nastavení kanálu")
            res = await mc.commands.get_channel(idx)
            _check(res, "načtení kanálu")
            return res.payload

        info = await self.manager.call(do)
        stored_secret = info.get("channel_secret")
        stored_secret = stored_secret.hex() if isinstance(stored_secret, (bytes, bytearray)) else stored_secret
        await self.repo.save_channel(idx, info.get("channel_name") or name, stored_secret)
        await self.repo.set_channel_region(idx, region)
        await self._publish_channels()
        return serialize.channel(await self.repo.get_channel(idx))

    async def delete_channel(self, idx: int, token_id: Optional[int] = None) -> None:
        async def go():
            async def do(mc):
                _check(await mc.commands.set_channel(idx, "", bytes(16)), "smazání kanálu")

            await self.manager.call(do)
            await self.repo.delete_channel(idx)
            await self._publish_channels()

        await self._run("channel_delete", {"idx": idx}, token_id, go())

    # -------------------------------------------------------------- node

    async def send_advert(self, flood: bool, token_id: Optional[int] = None) -> None:
        async def go():
            async def do(mc):
                _check(await mc.commands.send_advert(flood=flood), "odeslání advertu")

            await self.manager.call(do)
            logger.info("Advert odeslán (%s)", "flood" if flood else "zero-hop")

        await self._run("advert", {"flood": flood}, token_id, go())

    async def update_device(self, changes: dict, token_id: Optional[int] = None) -> dict:
        """changes: name, lat+lon, tx_power, radio {freq, bw, sf, cr}, default_region (None = zrušit)."""
        return await self._run("device_update", changes, token_id, self._update_device(changes))

    async def _update_device(self, changes: dict) -> dict:
        async def do(mc):
            if "name" in changes:
                _check(await mc.commands.set_name(changes["name"]), "změna jména")
            if "lat" in changes:
                _check(await mc.commands.set_coords(changes["lat"], changes["lon"]), "změna polohy")
            if "tx_power" in changes:
                _check(await mc.commands.set_tx_power(changes["tx_power"]), "změna výkonu")
            if "radio" in changes:
                r = changes["radio"]
                _check(await mc.commands.set_radio(r["freq"], r["bw"], r["sf"], r["cr"]), "změna parametrů rádia")
            if "default_region" in changes:
                # Knihovna u None padá; prázdný řetězec výchozí region zruší.
                region = changes["default_region"] or ""
                _check(await mc.commands.set_default_flood_scope(region), "změna výchozího regionu")
            # Načíst aktuální stav (SELF_INFO) zpět z companionu.
            res = await mc.commands.send_appstart()
            _check(res, "načtení stavu nodu")
            return dict(res.payload)

        info = await self.manager.call(do)
        status = self.manager.status
        status.self_info = info
        if "default_region" in changes:
            status.default_scope = changes["default_region"] or None
        await self.repo.save_device({**(await self._device_values()), "name": info.get("name")})
        config = {k: v for k, v in info.items() if k not in ("public_key", "name", "adv_type")}
        config["default_flood_scope"] = status.default_scope
        await self.repo.save_device_config(config)
        logger.info("Nastavení nodu změněno: %s", ", ".join(changes))
        device = serialize.device(await self.repo.get_device(), await self.repo.get_device_config())
        await self._publish("device.updated", device)
        return device

    async def _device_values(self) -> dict:
        row = await self.repo.get_device() or {}
        return {k: v for k, v in row.items() if k not in ("id", "first_seen_at", "updated_at", "last_connected_at")}

    async def resync(self, token_id: Optional[int] = None) -> None:
        await self._run("sync", {}, token_id, self.manager.resync())

    async def reboot(self, token_id: Optional[int] = None) -> None:
        async def go():
            async def do(mc):
                await mc.commands.reboot()  # companion neodpovídá, rovnou se restartuje

            await self.manager.call(do)
            logger.warning("Companion se restartuje na žádost API")

        await self._run("reboot", {}, token_id, go())

    # ------------------------------------------- přihlášení k room serveru / repeateru

    async def login(self, key: str, password: str, remember: bool = False, token_id: Optional[int] = None) -> dict:
        payload = {"public_key": key, "remember": remember}  # heslo do historie příkazů nepatří
        return await self._run("login", payload, token_id, self._login(key, password, remember))

    async def _login(self, key: str, password: str, remember: bool) -> dict:
        row = await self.repo.get_contact(key)
        if row is None:
            raise ActionError("kontakt v databázi neexistuje")
        if row.get("type") not in LOGIN_TYPES:
            raise ActionError("přihlásit se jde jen k room serveru nebo repeateru")
        # Companion potřebuje kontakt u sebe (kvůli šifrování).
        await self.ensure_on_device(key)

        async def send(mc):
            res = await mc.commands._send_login_raw(key, password)
            _check(res, "odeslání přihlášení")
            return res

        sent = await self.manager.call(send)
        timeout = (sent.payload or {}).get("suggested_timeout", 0) / 800
        timeout = min(max(timeout, LOGIN_MIN_TIMEOUT), LOGIN_MAX_TIMEOUT)
        prefix = key[:12]

        # Na odpověď čekáme mimo zámek – mezitím může server dál odesílat a přijímat.
        event = await self.manager.wait_for(
            (EventType.LOGIN_SUCCESS, EventType.LOGIN_FAILED),
            lambda e: (e.payload or {}).get("pubkey_prefix", prefix) == prefix,
            timeout,
        )
        name = row.get("name") or prefix
        if event is None or event.type == EventType.LOGIN_FAILED:
            error = "uzel neodpověděl" if event is None else "nesprávné heslo"
            await self.repo.save_login(key, logged_in=False, last_error=error)
            await self._publish_login(key)
            logger.warning("Přihlášení k %s selhalo: %s", name, error)
            raise ActionError(f"přihlášení selhalo: {error}")

        info = event.payload or {}
        values = {
            "logged_in": True,
            "is_admin": bool(info.get("is_admin")),
            "permissions": info.get("permissions"),
            "last_login_at": utcnow(),
            "last_error": None,
        }
        if remember:
            values.update(password=password, auto_login=True)
        await self.repo.save_login(key, **values)
        logger.info("Přihlášeno k %s%s", name, " (admin)" if values["is_admin"] else "")
        return await self._publish_login(key)

    async def logout(self, key: str, forget: bool = True, token_id: Optional[int] = None) -> dict:
        async def go():
            async def do(mc):
                await mc.commands.send_logout(key)  # i když selže, lokálně se odhlásíme

            try:
                await self.manager.call(do)
            except CompanionUnavailable:
                pass
            values = {"logged_in": False, "last_error": None}
            if forget:
                values.update(password=None, auto_login=False)
            await self.repo.save_login(key, **values)
            return await self._publish_login(key)

        return await self._run("logout", {"public_key": key, "forget": forget}, token_id, go())

    async def login_state(self, key: str) -> dict:
        row = await self.repo.get_login(key)
        return serialize.login(key, row)

    async def _publish_login(self, key: str) -> dict:
        state = await self.login_state(key)
        await self._publish("contact.login", state)
        return state

    async def on_state(self, status) -> None:
        """Po výpadku se přihlášení ruší; po připojení se obnoví ta zapamatovaná."""
        from .manager import ConnectionState

        if status.state == ConnectionState.DISCONNECTED:
            await self.repo.mark_all_logged_out()
        elif status.state == ConnectionState.CONNECTED:
            asyncio.create_task(self._relogin_all())

    async def _relogin_all(self) -> None:
        for entry in await self.repo.list_logins():
            if not entry.get("auto_login") or entry.get("password") is None:
                continue
            if not self.manager.connected.is_set():
                return
            await asyncio.sleep(RELOGIN_DELAY)
            try:
                await self._login(entry["contact_key"], entry["password"], remember=True)
            except (ActionError, CompanionUnavailable) as exc:
                logger.warning("Automatické přihlášení k %s selhalo: %s", entry["contact_key"][:12], exc)
            except Exception:  # noqa: BLE001
                logger.exception("Automatické přihlášení selhalo")
