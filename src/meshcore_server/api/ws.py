"""WebSocket /api/v1/ws – živé události s doplněním zmeškaných.

Připojení:  /api/v1/ws?token=<API token>      (server–server)
            /api/v1/ws?ticket=<lístek>         (prohlížeč, lístek z POST /ws-ticket)
            volitelně &since=<poslední seq, který klient zná>

Server pošle nejdřív {"type": "hello", "seq": <aktuální seq>}, pak případně
zmeškané události a dál živé. Když zmeškané události už nemá, pošle
{"type": "resync.required"} a klient si data načte znovu přes REST.
Klient může posílat {"type": "ping"}, server odpoví {"type": "pong"}.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from .. import __version__
from ..db.types import utcnow
from ..serialize import to_jsonable
from .context import ServerContext

logger = logging.getLogger("meshcore_server.ws")
router = APIRouter()

CLOSE_UNAUTHORIZED = 4401
CLOSE_TOO_SLOW = 4408
REPLAY_BATCH = 500


@router.websocket("/api/v1/ws")
async def websocket_endpoint(
    ws: WebSocket,
    token: Optional[str] = None,
    ticket: Optional[str] = None,
    since: Optional[int] = None,
):
    ctx: ServerContext = ws.app.state.ctx
    principal = ctx.tokens.redeem_ticket(ticket) if ticket else await ctx.tokens.authenticate(token)
    # Přijmeme a hned zavřeme s kódem 4401 – prohlížeč tak pozná neplatný token
    # (odmítnutí už při handshaku by viděl jen jako obecnou chybu).
    await ws.accept()
    if principal is None or not principal.allows("read"):
        await ws.close(code=CLOSE_UNAUTHORIZED, reason="unauthorized")
        return

    client = f"{ws.client.host}:{ws.client.port}" if ws.client else "?"
    # Odebírat začneme ještě před dohledáním zmeškaných, ať nic nepropadne mezi.
    sub = ctx.bus.subscribe({
        "ip": ws.client.host if ws.client else None,
        "token": principal.name,
        "connected_at": to_jsonable(utcnow()),
    })
    logger.info("WebSocket klient připojen: %s (token %s)", client, principal.name)
    try:
        await ws.send_json({"type": "hello", "seq": ctx.bus.last_seq, "server": __version__})
        last_sent = await _replay(ws, ctx, since) if since is not None else ctx.bus.last_seq

        receiver = asyncio.create_task(_receive(ws))
        try:
            while True:
                getter = asyncio.create_task(sub.queue.get())
                done, _ = await asyncio.wait({getter, receiver}, return_when=asyncio.FIRST_COMPLETED)
                if receiver in done:
                    getter.cancel()
                    break
                envelope = getter.result()
                if envelope["seq"] <= last_sent:
                    continue  # už odesláno při doplňování
                await ws.send_json(envelope)
                last_sent = envelope["seq"]
                if sub.overflowed and sub.queue.empty():
                    await ws.close(code=CLOSE_TOO_SLOW, reason="client too slow, reconnect with since")
                    break
        finally:
            receiver.cancel()
            if receiver.done() and not receiver.cancelled():
                receiver.exception()  # jen převzít výjimku (odpojení), ať ji asyncio nehlásí
    except WebSocketDisconnect:
        pass
    except Exception:  # noqa: BLE001
        logger.exception("Chyba WebSocketu (%s)", client)
    finally:
        ctx.bus.unsubscribe(sub)
        logger.info("WebSocket klient odpojen: %s", client)


async def _replay(ws: WebSocket, ctx: ServerContext, since: int) -> int:
    """Pošle události se seq > since. Vrátí seq poslední odeslané."""
    min_seq, _ = await ctx.repo.event_seq_bounds()
    last = since
    if since < ctx.bus.last_seq and (min_seq is None or since < min_seq - 1):
        await ws.send_json({"type": "resync.required", "seq": ctx.bus.last_seq})
        return ctx.bus.last_seq
    while True:
        rows = await ctx.repo.events_after(last, limit=REPLAY_BATCH)
        for row in rows:
            await ws.send_json(to_jsonable({
                "seq": row["seq"], "type": row["type"], "ts": row["created_at"], "data": row["data"],
            }))
            last = row["seq"]
        if len(rows) < REPLAY_BATCH:
            return last


async def _receive(ws: WebSocket) -> None:
    """Čte zprávy od klienta (ping); skončí, když se klient odpojí."""
    while True:
        raw = await ws.receive_text()
        try:
            msg = json.loads(raw)
        except ValueError:
            continue
        if isinstance(msg, dict) and msg.get("type") == "ping":
            await ws.send_json({"type": "pong"})
