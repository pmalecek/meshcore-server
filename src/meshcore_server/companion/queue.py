"""Fronta příkazů pro companion.

Příkazy z API se ukládají do tabulky commands a provádějí se postupně, jeden
po druhém, jen když je companion připojený. Fronta je v DB, takže přežije
restart serveru; rozpracované příkazy se po startu vrátí do fronty.

Odesílání zpráv:
  kanál – nastaví region kanálu (flood scope), odešle, vrátí výchozí region.
          Kanálové zprávy nemají ACK, končí stavem "sent".
  DM    – odešle a čeká na ACK. Bez ACK zkusí znovu (stejný čas zprávy, vyšší
          číslo pokusu), před posledním pokusem přepne kontakt na flood.
          Pozdní ACK kteréhokoli pokusu zprávu potvrdí.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Optional

from meshcore import EventType

from .. import serialize
from ..db.repo import Repository
from ..db.types import utcnow
from .manager import CompanionUnavailable, is_timeout
from .scope import ScopeError, with_scope

if TYPE_CHECKING:
    from ..config import CompanionConfig
    from ..events import EventBus
    from ..hops import HopResolver
    from .manager import CompanionManager

logger = logging.getLogger("meshcore_server.queue")

EXPIRY_CHECK_INTERVAL = 5.0   # s
MIN_ACK_TIMEOUT = 5.0         # s
MAX_ACK_TIMEOUT = 60.0        # s
REPEAT_WINDOW = 120.0         # s, jak dlouho po odeslání počítat opakování repeatery


def channel_msg_hash(sender_ts: int, full_text: str) -> int:
    """Hash kanálové zprávy, jak ho počítá knihovna meshcore z RX logu.

    full_text je text tak, jak jde do éteru: „jméno: zpráva“.
    """
    digest = hashlib.sha256(int(sender_ts).to_bytes(4, "little") + full_text.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "little")


class CommandError(Exception):
    """Příkaz selhal z důvodu, který nemá smysl opakovat."""


class CommandQueue:
    def __init__(
        self,
        repo: Repository,
        manager: "CompanionManager",
        cfg: "CompanionConfig",
        bus: Optional["EventBus"] = None,
        hops: Optional["HopResolver"] = None,
        actions=None,
    ) -> None:
        self.actions = actions  # DeviceActions – přidání kontaktu do companionu před DM
        self.repo = repo
        self.manager = manager
        self.cfg = cfg
        self.bus = bus
        self.hops = hops
        self._wake = asyncio.Event()
        self._ack_map: dict[str, int] = {}          # kód ACK → id zprávy (všechny pokusy)
        self._ack_timers: dict[int, asyncio.TimerHandle] = {}
        # hash odeslané kanálové zprávy → (id zprávy, konec sledování, repeatery)
        self._repeat_watch: dict[int, tuple[int, float, list[str]]] = {}
        manager.add_event_handler(self._on_event)

    # ------------------------------------------------------------- API

    def wake(self) -> None:
        self._wake.set()

    async def enqueue_message(self, message: dict, token_id: Optional[int]) -> tuple[dict, int]:
        """Uloží odchozí zprávu do DB a zařadí ji k odeslání. Vrátí (zpráva, id příkazu)."""
        msg_id, cmd_id = await self.repo.create_outgoing_message(message, token_id)
        row = await self.repo.get_message(msg_id)
        data = await serialize.message_with_hops(row, self.hops)
        await self._publish("message.created", data)
        self.wake()
        return data, cmd_id

    async def run(self, stop: asyncio.Event) -> None:
        requeued = await self.repo.requeue_running_commands()
        if requeued:
            logger.info("Fronta: %d rozpracovaných příkazů vráceno k provedení", requeued)
        stop_task = asyncio.create_task(stop.wait())
        try:
            while not stop.is_set():
                await self._expire()
                if self.manager.connected.is_set():
                    cmd = await self.repo.next_pending_command()
                    if cmd is not None:
                        await self._execute(cmd)
                        continue
                # Nic k provedení (nebo companion offline): počkat na probuzení.
                self._wake.clear()
                waiters = [stop_task, asyncio.create_task(self._wake.wait()),
                           asyncio.create_task(self.manager.connected.wait())
                           if not self.manager.connected.is_set() else None]
                waiters = [w for w in waiters if w is not None]
                await asyncio.wait(waiters, timeout=EXPIRY_CHECK_INTERVAL, return_when=asyncio.FIRST_COMPLETED)
                for w in waiters:
                    if w is not stop_task:
                        w.cancel()
        finally:
            stop_task.cancel()
            for handle in self._ack_timers.values():
                handle.cancel()

    # -------------------------------------------------------- provádění

    async def _execute(self, cmd: dict) -> None:
        cmd_id = cmd["id"]
        await self.repo.update_command(cmd_id, status="running", started_at=utcnow())
        try:
            if cmd["type"] == "send_msg":
                result = await self._send_message(cmd)
            else:
                raise CommandError(f"neznámý příkaz {cmd['type']!r}")
        except CompanionUnavailable as exc:
            # Companion neodpověděl nebo spojení spadlo – příkaz se zkusí znovu.
            logger.warning("Příkaz %d odložen: %s", cmd_id, exc)
            await self.repo.update_command(cmd_id, status="pending", started_at=None)
            await asyncio.sleep(2)  # ať se při potížích nezacyklíme
            return
        except CommandError as exc:
            await self._finish(cmd_id, "failed", str(exc))
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("Příkaz %d selhal", cmd_id)
            await self._finish(cmd_id, "failed", f"interní chyba: {exc}")
            return
        await self._finish(cmd_id, "done", None, result)

    async def _finish(self, cmd_id: int, status: str, error: Optional[str], result: Any = None) -> None:
        await self.repo.update_command(cmd_id, status=status, error=error, finished_at=utcnow())
        await self._publish("command.finished", {"id": cmd_id, "status": status, "error": error, "result": result})

    async def _send_message(self, cmd: dict) -> dict:
        msg_id = (cmd.get("payload") or {}).get("message_id")
        msg = await self.repo.get_message(msg_id) if msg_id else None
        if msg is None:
            raise CommandError("zpráva neexistuje")
        if msg["status"] in ("acked", "failed"):
            return {"message_id": msg_id, "status": msg["status"]}
        expired = msg["expires_at"] is not None and msg["expires_at"] < utcnow()
        if expired and msg["status"] == "pending":
            await self._message_failed(msg_id, "vypršel čas na odeslání")
            raise CommandError("vypršel čas na odeslání")
        if expired and msg["status"] == "sent":
            # Opakování DM, které čekalo, než se companion znovu připojí.
            await self._message_failed(msg_id, "příjemce nepotvrdil doručení")
            raise CommandError("příjemce nepotvrdil doručení")

        attempt = msg["attempts"] or 0
        try:
            if msg["kind"] == "channel":
                await self._send_channel(msg)
                await self._message_status(msg_id, status="sent", attempts=attempt + 1, error=None)
            else:
                await self._send_dm(msg, attempt)
        except CommandError as exc:
            await self._message_failed(msg_id, str(exc), attempts=attempt + 1)
            raise
        return {"message_id": msg_id, "attempt": attempt + 1}

    async def _send_channel(self, msg: dict) -> None:
        channel = await self.repo.get_channel(msg["channel_idx"])
        if channel is None:
            raise CommandError(f"kanál {msg['channel_idx']} v companionu neexistuje")
        region = channel.get("region")

        async def do(mc):
            try:
                return await with_scope(
                    mc, region, lambda: mc.commands.send_chan_msg(msg["channel_idx"], msg["text"], msg["sender_ts"])
                )
            except ScopeError as exc:
                raise CommandError(str(exc)) from exc

        res = await self.manager.call(do)
        self._check_result(res, "odeslání do kanálu")
        if msg.get("sender_name") is not None:
            full = f"{msg['sender_name']}: {msg['text']}"
            self._expire_repeat_watch()
            self._repeat_watch[channel_msg_hash(msg["sender_ts"], full)] = (
                msg["id"], time.monotonic() + REPEAT_WINDOW, []
            )

    async def _send_dm(self, msg: dict, attempt: int) -> None:
        key = msg.get("contact_key")
        if not key:
            raise CommandError("kontakt nemá známý public key")
        mc = self.manager.mc
        if self.actions is not None and mc is not None and key not in mc.contacts and self.cfg.auto_add_contacts_on_send:
            from .actions import ActionError

            try:
                await self.actions.ensure_on_device(key)
            except ActionError as exc:
                raise CommandError(f"kontakt nejde přidat do companionu: {exc}") from exc

        # region kontaktu (platí jen pro odeslání floodem – bez známé trasy nebo po jejím resetu)
        region = (await self.repo.get_contact(key) or {}).get("region")

        async def do(mc):
            if key not in mc.contacts:
                raise CommandError("kontakt není v companionu")
            # Poslední pokus jde floodem: přímá trasa zřejmě přestala fungovat.
            if attempt == self.cfg.dm_max_attempts - 1 and attempt > 0:
                contact = mc.contacts[key]
                if contact.get("out_path_len", -1) >= 0:
                    logger.info("DM %d: reset trasy ke kontaktu, poslední pokus floodem", msg["id"])
                    await mc.commands.reset_path(key)
            try:
                return await with_scope(
                    mc, region, lambda: mc.commands.send_msg(key, msg["text"], msg["sender_ts"], attempt=attempt)
                )
            except ScopeError as exc:
                raise CommandError(str(exc)) from exc

        res = await self.manager.call(do)
        self._check_result(res, "odeslání DM")
        ack = res.payload["expected_ack"]
        ack = ack.hex() if isinstance(ack, (bytes, bytearray)) else str(ack)
        self._ack_map[ack] = msg["id"]
        timeout = res.payload.get("suggested_timeout", 0) / 1000 * 1.2
        timeout = min(max(timeout, MIN_ACK_TIMEOUT), MAX_ACK_TIMEOUT)
        await self._message_status(
            msg["id"], status="sent", attempts=attempt + 1, expected_ack=ack, error=None,
        )
        self._arm_ack_timer(msg["id"], timeout)

    @staticmethod
    def _check_result(res, what: str) -> None:
        if is_timeout(res):
            raise CompanionUnavailable(f"{what}: companion neodpověděl")
        if res.type == EventType.ERROR:
            payload = res.payload or {}
            reason = payload.get("code_string") or payload.get("reason") or payload.get("error_code")
            raise CommandError(f"{what} selhalo: {reason}")

    # ------------------------------------------------------------- ACK

    def _arm_ack_timer(self, msg_id: int, timeout: float) -> None:
        old = self._ack_timers.pop(msg_id, None)
        if old:
            old.cancel()
        loop = asyncio.get_running_loop()
        self._ack_timers[msg_id] = loop.call_later(
            timeout, lambda: asyncio.create_task(self._ack_timeout(msg_id))
        )

    async def _ack_timeout(self, msg_id: int) -> None:
        self._ack_timers.pop(msg_id, None)
        msg = await self.repo.get_message(msg_id)
        if msg is None or msg["status"] != "sent":
            return
        if (msg["attempts"] or 0) < self.cfg.dm_max_attempts:
            logger.info("DM %d bez potvrzení, pokus %d/%d", msg_id, msg["attempts"] + 1, self.cfg.dm_max_attempts)
            await self.repo.insert_command("send_msg", {"message_id": msg_id, "retry": True})
            self.wake()
        else:
            await self._message_failed(msg_id, "příjemce nepotvrdil doručení")

    async def _on_event(self, event) -> None:
        if event.type == EventType.RX_LOG_DATA:
            await self._on_rx_log(event.payload or {})
            return
        if event.type != EventType.ACK:
            return
        code = (event.payload or {}).get("code")
        if not code:
            return
        msg_id = self._ack_map.pop(code, None)
        msg = await self.repo.get_message(msg_id) if msg_id else await self.repo.find_message_by_ack(code)
        if msg is None or msg["status"] == "acked":
            return
        timer = self._ack_timers.pop(msg["id"], None)
        if timer:
            timer.cancel()
        # Pozdní ACK potvrdí i zprávu, která už byla vzdána.
        await self._message_status(msg["id"], status="acked", error=None)
        logger.info("DM %d potvrzena", msg["id"])

    # ------------------------------------------------- opakování repeatery

    def _expire_repeat_watch(self) -> None:
        now = time.monotonic()
        for key in [k for k, (_, until, _) in self._repeat_watch.items() if until < now]:
            del self._repeat_watch[key]

    async def _on_rx_log(self, log: dict) -> None:
        """Companion zachytil paket; je to opakování naší kanálové zprávy?"""
        if not self._repeat_watch or log.get("msg_hash") is None:
            return
        entry = self._repeat_watch.get(log["msg_hash"])
        if entry is None:
            return
        msg_id, until, repeaters = entry
        if time.monotonic() > until:
            del self._repeat_watch[log["msg_hash"]]
            return
        path = log.get("path") or ""
        if isinstance(path, (bytes, bytearray)):
            path = path.hex()
        size = int(log.get("path_hash_size") or 1) * 2
        if not path or log.get("path_len", 0) <= 0:
            return  # bez trasy to není opakování repeaterem
        last_hop = path[-size:]
        if last_hop not in repeaters:
            repeaters.append(last_hop)
        msg = await self.repo.get_message(msg_id)
        if msg is None:
            return
        await self.repo.update_message(
            msg_id, heard_repeats=(msg["heard_repeats"] or 0) + 1, heard_by=",".join(repeaters)[:255]
        )
        await self._publish_status(msg_id)

    # ------------------------------------------------------ stavy zpráv

    async def _message_status(self, msg_id: int, **values) -> None:
        await self.repo.update_message(msg_id, **values)
        await self._publish_status(msg_id)

    async def _publish_status(self, msg_id: int) -> None:
        msg = await self.repo.get_message(msg_id)
        if msg:
            await self._publish("message.status", {
                "id": msg_id,
                "status": msg["status"],
                "attempts": msg["attempts"],
                "expected_ack": msg["expected_ack"],
                "error": msg["error"],
                "heard_repeats": msg.get("heard_repeats") or 0,
                "heard_by": await serialize.heard_by(msg, self.hops),
            })

    async def _message_failed(self, msg_id: int, error: str, **values) -> None:
        await self._message_status(msg_id, status="failed", error=error, **values)

    async def _expire(self) -> None:
        for msg_id in await self.repo.expire_pending_messages(utcnow()):
            msg = await self.repo.get_message(msg_id)
            if msg:
                await self._publish("message.status", {
                    "id": msg_id, "status": "failed", "attempts": msg["attempts"],
                    "expected_ack": None, "error": msg["error"],
                })

    async def _publish(self, type_: str, data: Any) -> None:
        if self.bus is not None:
            await self.bus.publish(type_, data)
