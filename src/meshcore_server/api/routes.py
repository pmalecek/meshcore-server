"""REST endpointy pro čtení (krok 3)."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional, Union

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import text

from .. import __version__, serialize
from ..auth import TICKET_TTL, Principal
from ..db.types import utcnow
from .context import ServerContext
from .deps import get_ctx, require

router = APIRouter(prefix="/api/v1")
health_router = APIRouter()

MAX_LIMIT = 500


@health_router.get("/health", include_in_schema=False)
async def health(ctx: ServerContext = Depends(get_ctx)):
    """Pro healthcheck kontejneru. Bez autentizace, neprozrazuje nic citlivého.

    Výpadek companionu není chyba serveru (jinak by se kontejner restartoval
    pokaždé, když vypadne WiFi rádia), chybou je jen nedostupná databáze.
    """
    try:
        async with ctx.db.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:  # noqa: BLE001
        return JSONResponse({"status": "error", "database": "unavailable"}, status_code=503)
    return {"status": "ok", "companion": ctx.companion.status.state.value}


@router.get("/status")
async def get_status(ctx: ServerContext = Depends(get_ctx), _: Principal = Depends(require("read"))):
    st = ctx.companion.status
    return serialize.to_jsonable({
        "server": {"version": __version__, "uptime": int(time.time() - ctx.started_at)},
        "companion": {
            "state": st.state.value,
            "transport": ctx.cfg.companion.transport,
            "connected_since": datetime.fromtimestamp(st.connected_since, tz=timezone.utc) if st.connected_since else None,
            "reconnects": st.reconnects,
            "last_error": st.last_error,
            "name": st.self_info.get("name"),
            "public_key": st.self_info.get("public_key"),
        },
        "events": {"last_seq": ctx.bus.last_seq},
    })


@router.get("/device")
async def get_device(ctx: ServerContext = Depends(get_ctx), _: Principal = Depends(require("read"))):
    row = await ctx.repo.get_device()
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "companion zatím nebyl připojen")
    return serialize.device(row, await ctx.repo.get_device_config())


@router.get("/contacts")
async def list_contacts(
    type: Optional[int] = Query(None, description="1 chat, 2 repeater, 3 room, 4 senzor"),
    on_device: Optional[bool] = None,
    q: Optional[str] = Query(None, max_length=64, description="hledání ve jméně"),
    ctx: ServerContext = Depends(get_ctx),
    _: Principal = Depends(require("read")),
):
    rows = await ctx.repo.list_contacts(type_=type, on_device=on_device, q=q)
    return {"items": [serialize.contact(r) for r in rows]}


@router.get("/contacts/{key}")
async def get_contact(key: str, ctx: ServerContext = Depends(get_ctx), _: Principal = Depends(require("read"))):
    key = key.lower()
    row = await ctx.repo.get_contact(key) if len(key) == 64 else await ctx.repo.find_contact_by_prefix(key)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "kontakt nenalezen")
    return serialize.contact(row)


@router.get("/channels")
async def list_channels(ctx: ServerContext = Depends(get_ctx), _: Principal = Depends(require("read"))):
    return {"items": [serialize.channel(r) for r in await ctx.repo.list_channels()]}


@router.get("/messages")
async def list_messages(
    kind: Optional[Literal["dm", "channel"]] = None,
    peer: Optional[str] = Query(None, description="DM: prefix klíče kontaktu, kanál: číslo kanálu"),
    before: Optional[int] = Query(None, description="jen zprávy s id menším než toto (starší)"),
    after: Optional[int] = Query(None, description="jen zprávy s id větším než toto (novější)"),
    limit: int = Query(50, ge=1, le=MAX_LIMIT),
    ctx: ServerContext = Depends(get_ctx),
    _: Principal = Depends(require("read")),
):
    contact_prefix = channel_idx = None
    if peer is not None:
        if kind == "channel":
            try:
                channel_idx = int(peer)
            except ValueError:
                raise HTTPException(422, "peer pro kanál musí být číslo")
        elif kind == "dm":
            contact_prefix = peer
        else:
            raise HTTPException(422, "peer vyžaduje kind=dm nebo kind=channel")

    rows = await ctx.repo.list_messages(
        kind=kind, contact_prefix=contact_prefix, channel_idx=channel_idx,
        before=before, after=after, limit=limit,
    )
    items = [await serialize.message_with_hops(r, ctx.hops) for r in rows]
    return {
        "items": items,
        # kurzor pro další (starší) stránku
        "next_before": items[-1]["id"] if len(items) == limit else None,
    }


@router.get("/adverts")
async def list_adverts(
    key: Optional[str] = Query(None, description="public key nebo jeho prefix"),
    before: Optional[int] = None,
    limit: int = Query(50, ge=1, le=MAX_LIMIT),
    ctx: ServerContext = Depends(get_ctx),
    _: Principal = Depends(require("read")),
):
    rows = await ctx.repo.list_adverts(public_key=key, before=before, limit=limit)
    items = [serialize.advert(r) for r in rows]
    return {"items": items, "next_before": items[-1]["id"] if len(items) == limit else None}


@router.post("/ws-ticket")
async def ws_ticket(ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("read"))):
    """Krátkodobý jednorázový lístek pro WebSocket z prohlížeče (token tak zůstane na serveru klienta)."""
    return {"ticket": ctx.tokens.issue_ticket(principal), "expires_in": int(TICKET_TTL)}


# ---------------------------------------------------------------- zápisy


class SendMessageRequest(BaseModel):
    kind: Literal["channel", "dm"]
    to: Union[int, str] = Field(description="kanál: číslo kanálu; DM: public key kontaktu nebo jeho prefix")
    text: str = Field(min_length=1)


@router.post("/messages", status_code=status.HTTP_202_ACCEPTED)
async def send_message(
    body: SendMessageRequest,
    ctx: ServerContext = Depends(get_ctx),
    principal: Principal = Depends(require("write")),
):
    """Zařadí zprávu k odeslání. Průběh hlásí události message.status a command.finished."""
    device = await ctx.repo.get_device()
    if device is None:
        raise HTTPException(status.HTTP_409_CONFLICT, "companion zatím nebyl připojen")
    node_name = ctx.companion.status.self_info.get("name") or device.get("name") or ""
    limit = ctx.cfg.companion.max_text_bytes
    text_bytes = len(body.text.encode("utf-8"))
    now = utcnow()
    msg = {
        "direction": "out",
        "sender_name": node_name[:64] or None,
        "text": body.text,
        "txt_type": 0,
        "sender_ts": int(time.time()),
        "received_at": now,  # u odchozí zprávy čas vytvoření
        "status": "pending",
        "attempts": 0,
        "expires_at": now + timedelta(minutes=ctx.cfg.companion.message_expiry_minutes),
        "dedup_hash": "out:" + uuid.uuid4().hex,
    }

    if body.kind == "channel":
        try:
            idx = int(body.to)
        except (TypeError, ValueError):
            raise HTTPException(422, "to musí být číslo kanálu")
        if await ctx.repo.get_channel(idx) is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"kanál {idx} neexistuje")
        # Firmware posílá „jméno: text“ a co se nevejde, tiše ořízne.
        allowed = limit - len(f"{node_name}: ".encode("utf-8"))
        if text_bytes > allowed:
            raise HTTPException(
                422,
                f"text je příliš dlouhý ({text_bytes} B), do kanálu se vejde nejvýš {allowed} B",
            )
        msg.update(kind="channel", channel_idx=idx)
    else:
        key = str(body.to).lower()
        contact = await ctx.repo.get_contact(key) if len(key) == 64 else await ctx.repo.find_contact_by_prefix(key)
        if contact is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "kontakt nenalezen")
        if not contact["on_device"] and not ctx.cfg.companion.auto_add_contacts_on_send:
            raise HTTPException(status.HTTP_409_CONFLICT, "kontakt není v companionu")
        if text_bytes > limit:
            raise HTTPException(
                422, f"text je příliš dlouhý ({text_bytes} B), limit je {limit} B"
            )
        msg.update(
            kind="dm",
            contact_key=contact["public_key"],
            contact_prefix=contact["public_key"][:12],
        )

    data, cmd_id = await ctx.queue.enqueue_message(msg, principal.token_id)
    return {"message": data, "command_id": cmd_id}


@router.get("/commands/{cmd_id}")
async def get_command(cmd_id: int, ctx: ServerContext = Depends(get_ctx), _: Principal = Depends(require("read"))):
    row = await ctx.repo.get_command(cmd_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "příkaz nenalezen")
    return serialize.command(row)


class ChannelUpdateRequest(BaseModel):
    region: Optional[str] = Field(
        None, max_length=31, description="region (flood scope) kanálu, např. #plzen; null = výchozí region zařízení"
    )


@router.patch("/channels/{idx}")
async def update_channel(
    idx: int,
    body: ChannelUpdateRequest,
    ctx: ServerContext = Depends(get_ctx),
    _: Principal = Depends(require("write")),
):
    """Nastaví region kanálu. Ukládá se jen na serveru; companion si region kanálu nepamatuje."""
    region = (body.region or "").strip() or None
    if region and not region.startswith("#"):
        region = "#" + region
    if region and len(region.encode("utf-8")) > 31:
        raise HTTPException(422, "region může mít nejvýš 31 bajtů")
    if not await ctx.repo.set_channel_region(idx, region):
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"kanál {idx} neexistuje")
    channels = await ctx.repo.list_channels()
    await ctx.bus.publish("channels.updated", [serialize.channel(r) for r in channels])
    return serialize.channel(next(r for r in channels if r["idx"] == idx))


# ------------------------------------------------------- správa kontaktů


async def _contact_or_404(ctx: ServerContext, key: str) -> dict:
    key = key.lower()
    row = await ctx.repo.get_contact(key) if len(key) == 64 else await ctx.repo.find_contact_by_prefix(key)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "kontakt nenalezen")
    return row


async def _action(coro):
    """Převede chyby akcí na HTTP odpovědi."""
    from ..companion import CompanionUnavailable
    from ..companion.actions import ActionError

    try:
        return await coro
    except CompanionUnavailable as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc))
    except ActionError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc))


class ContactUpdateRequest(BaseModel):
    favorite: Optional[bool] = Field(None, description="oblíbený kontakt se z companionu nikdy neodebere")


@router.patch("/contacts/{key}")
async def update_contact(
    key: str, body: ContactUpdateRequest,
    ctx: ServerContext = Depends(get_ctx), _: Principal = Depends(require("write")),
):
    row = await _contact_or_404(ctx, key)
    if body.favorite is not None:
        return await ctx.actions.set_favorite(row["public_key"], body.favorite)
    return serialize.contact(row)


@router.post("/contacts/{key}/device")
async def add_contact_to_device(
    key: str, ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("write")),
):
    """Přidá kontakt z databáze do companionu. Je-li companion plný, odebere nejdéle neaktivní kontakt."""
    row = await _contact_or_404(ctx, key)
    return await _action(ctx.actions.add_to_device(row["public_key"], principal.token_id))


@router.delete("/contacts/{key}/device")
async def remove_contact_from_device(
    key: str, ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("write")),
):
    """Odebere kontakt z companionu; v databázi zůstane (on_device = false)."""
    row = await _contact_or_404(ctx, key)
    return await _action(ctx.actions.remove_from_device(row["public_key"], principal.token_id))


@router.delete("/contacts/{key}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_contact(
    key: str, ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("admin")),
):
    """Smaže kontakt z companionu i z databáze. Historie zpráv zůstane."""
    row = await _contact_or_404(ctx, key)
    await _action(ctx.actions.delete_contact(row["public_key"], principal.token_id))


@router.post("/contacts/{key}/reset-path")
async def reset_contact_path(
    key: str, ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("write")),
):
    """Zapomene přímou trasu ke kontaktu; další DM půjde floodem a trasa se najde znovu."""
    row = await _contact_or_404(ctx, key)
    return await _action(ctx.actions.reset_path(row["public_key"], principal.token_id))


@router.get("/settings/contacts")
async def get_contact_settings(ctx: ServerContext = Depends(get_ctx), _: Principal = Depends(require("read"))):
    device = await ctx.repo.get_device() or {}
    config = await ctx.repo.get_device_config()
    return {
        "manual_add_contacts": bool(config.get("manual_add_contacts")),
        "auto_add_contacts_on_send": ctx.cfg.companion.auto_add_contacts_on_send,
        "max_contacts": device.get("max_contacts"),
        "on_device": await ctx.repo.count_on_device(),
    }


class ContactSettingsRequest(BaseModel):
    manual_add_contacts: bool = Field(
        description="true = companion nové kontakty z advertů sám nepřidává (server je přesto uloží do DB)"
    )


@router.patch("/settings/contacts")
async def update_contact_settings(
    body: ContactSettingsRequest,
    ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("admin")),
):
    await _action(ctx.actions.set_manual_add(body.manual_add_contacts, principal.token_id))
    return await get_contact_settings(ctx, principal)


# --------------------------------------------------------- správa kanálů


class ChannelSetRequest(BaseModel):
    name: str = Field(min_length=1, max_length=31, description="#nazev = hashtag kanál (klíč se odvodí ze jména)")
    secret: Optional[str] = Field(
        None, pattern="^[0-9a-fA-F]{32}$", description="klíč soukromého kanálu, 32 hex znaků (16 B)"
    )
    region: Optional[str] = Field(None, max_length=31, description="region (flood scope), např. #plzen")


@router.put("/channels/{idx}")
async def set_channel(
    idx: int, body: ChannelSetRequest,
    ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("write")),
):
    """Nastaví kanál ve slotu idx (vytvoří nebo přepíše)."""
    name = body.name.strip()
    if len(name.encode("utf-8")) > 31:
        raise HTTPException(422, "jméno kanálu může mít nejvýš 31 bajtů")
    if not name.startswith("#") and not body.secret:
        raise HTTPException(422, "soukromý kanál (jméno bez #) potřebuje secret; hashtag kanál začíná #")
    region = (body.region or "").strip() or None
    if region and not region.startswith("#"):
        region = "#" + region
    secret = None if name.startswith("#") else body.secret.lower()
    return await _action(ctx.actions.set_channel(idx, name, secret, region, principal.token_id))


@router.delete("/channels/{idx}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_channel(
    idx: int, ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("write")),
):
    if await ctx.repo.get_channel(idx) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"kanál {idx} neexistuje")
    await _action(ctx.actions.delete_channel(idx, principal.token_id))


# ------------------------------------------------------------ správa nodu

LORA_BANDWIDTHS = (7.8, 10.4, 15.6, 20.8, 31.25, 41.7, 62.5, 125.0, 250.0, 500.0)


class AdvertRequest(BaseModel):
    flood: bool = Field(False, description="true = flood přes repeatery, false = jen sousedům (zero-hop)")


@router.post("/device/advert")
async def send_advert(
    body: AdvertRequest = AdvertRequest(),
    ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("write")),
):
    await _action(ctx.actions.send_advert(body.flood, principal.token_id))
    return {"status": "sent", "flood": body.flood}


class RadioParams(BaseModel):
    freq: float = Field(ge=150, le=960, description="frekvence v MHz, např. 869.525")
    bw: float = Field(description="šířka pásma v kHz: 7.8, 10.4, 15.6, 20.8, 31.25, 41.7, 62.5, 125, 250, 500")
    sf: int = Field(ge=5, le=12, description="spreading factor")
    cr: int = Field(ge=5, le=8, description="coding rate 4/5 až 4/8")


class DeviceUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=31)
    lat: Optional[float] = Field(None, ge=-90, le=90)
    lon: Optional[float] = Field(None, ge=-180, le=180)
    tx_power: Optional[int] = Field(None, ge=1, le=30, description="dBm, nejvýš max_tx_power zařízení")
    radio: Optional[RadioParams] = None
    default_region: Optional[str] = Field(
        None, max_length=31, description="výchozí region (flood scope) zařízení; prázdný řetězec = zrušit"
    )


@router.patch("/device")
async def update_device(
    body: DeviceUpdateRequest,
    ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("admin")),
):
    """Změní nastavení nodu. Pozor: jiné parametry rádia než okolní síť node od sítě odříznou."""
    sent = body.model_dump(exclude_unset=True)
    changes: dict = {}
    if "name" in sent and body.name is not None:
        name = body.name.strip()
        if not name or len(name.encode("utf-8")) > 31:
            raise HTTPException(422, "jméno musí mít 1–31 bajtů")
        changes["name"] = name
    if ("lat" in sent) != ("lon" in sent) or (body.lat is None) != (body.lon is None):
        raise HTTPException(422, "lat a lon se zadávají společně")
    if body.lat is not None:
        changes["lat"], changes["lon"] = body.lat, body.lon
    if body.tx_power is not None:
        max_tx = ctx.companion.status.self_info.get("max_tx_power")
        if max_tx and body.tx_power > max_tx:
            raise HTTPException(422, f"výkon může být nejvýš {max_tx} dBm")
        changes["tx_power"] = body.tx_power
    if body.radio is not None:
        if not any(abs(body.radio.bw - bw) < 0.05 for bw in LORA_BANDWIDTHS):
            raise HTTPException(422, f"šířka pásma musí být jedna z {', '.join(str(b) for b in LORA_BANDWIDTHS)}")
        changes["radio"] = body.radio.model_dump()
    if "default_region" in sent:
        region = (body.default_region or "").strip() or None
        if region and not region.startswith("#"):
            region = "#" + region
        if region and len(region.encode("utf-8")) > 31:
            raise HTTPException(422, "region může mít nejvýš 31 bajtů")
        changes["default_region"] = region
    if not changes:
        raise HTTPException(422, "nic ke změně")
    return await _action(ctx.actions.update_device(changes, principal.token_id))


@router.post("/sync")
async def force_sync(ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("write"))):
    """Znovu načte info o nodu, kontakty a kanály z companionu."""
    await _action(ctx.actions.resync(principal.token_id))
    return {"status": "synced"}


@router.post("/device/reboot", status_code=status.HTTP_202_ACCEPTED)
async def reboot_device(ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("admin"))):
    """Restartuje companion. Server se po restartu připojí znovu sám."""
    await _action(ctx.actions.reboot(principal.token_id))
    return {"status": "rebooting"}


# --------------------------------------------------------- stav přečtení

READER_PATTERN = r"^[\w.@+-]{0,64}$"
CONVERSATION_PATTERN = r"^(dm:[0-9a-f]{12}|ch:\d{1,3})$"


def _reader_key(principal: Principal, reader: Optional[str]) -> str:
    """Čtenář je vždy vázaný na token: <token>:<uživatel klienta>."""
    return f"{principal.name}:{reader or ''}"


@router.get("/unread")
async def get_unread(
    reader: Optional[str] = Query(None, pattern=READER_PATTERN, description="uživatel klienta, např. jeho ID"),
    ctx: ServerContext = Depends(get_ctx),
    principal: Principal = Depends(require("read")),
):
    """Počty nepřečtených příchozích zpráv podle konverzací (dm:<prefix>, ch:<číslo>)."""
    key = _reader_key(principal, reader)
    state = await ctx.repo.get_read_state(key)
    items = []
    for conv in await ctx.repo.conversations():
        last_read = state.get(conv["conversation"], 0)
        unread = 0
        if conv["last_message_id"] > last_read:
            unread = conv["total"] if last_read == 0 else await ctx.repo.count_unread(conv["conversation"], last_read)
        items.append({**conv, "last_read_message_id": last_read or None, "unread": unread})
    items.sort(key=lambda c: c["last_message_id"], reverse=True)
    return {"reader": reader or "", "total_unread": sum(c["unread"] for c in items), "items": items}


class ReadRequest(BaseModel):
    conversation: str = Field(pattern=CONVERSATION_PATTERN, description="dm:<prefix 12 hex> nebo ch:<číslo kanálu>")
    message_id: int = Field(ge=0, description="id poslední přečtené zprávy")
    reader: Optional[str] = Field(None, pattern=READER_PATTERN)
    force: bool = Field(False, description="povolit posun zpět (označit jako nepřečtené)")


@router.post("/read")
async def mark_read(
    body: ReadRequest, ctx: ServerContext = Depends(get_ctx), principal: Principal = Depends(require("read")),
):
    """Označí konverzaci jako přečtenou po zprávu message_id (bez force jen dopředu)."""
    key = _reader_key(principal, body.reader)
    value = await ctx.repo.set_read_state(key, body.conversation, body.message_id, body.force)
    data = {"reader": body.reader or "", "token": principal.name,
            "conversation": body.conversation, "last_read_message_id": value}
    # Ostatní zařízení téhož uživatele si podle toho aktualizují počty.
    await ctx.bus.publish("read.updated", data)
    return data


# ------------------------------------------------------------ stav serveru


def _ts(value: Optional[float]):
    return datetime.fromtimestamp(value, tz=timezone.utc) if value else None


def _process_stats() -> dict:
    import os

    import psutil

    proc = _process_stats.proc  # type: ignore[attr-defined]
    with proc.oneshot():
        mem = proc.memory_info()
        data = {
            "cpu_percent": proc.cpu_percent(interval=None),  # od minulého volání
            "memory_rss_mb": round(mem.rss / 1024 / 1024, 1),
            "threads": proc.num_threads(),
        }
    try:
        data["system_load"] = [round(x, 2) for x in os.getloadavg()]
    except (AttributeError, OSError):
        data["system_load"] = None
    vm = psutil.virtual_memory()
    data["system_memory_percent"] = vm.percent
    data["cpu_count"] = psutil.cpu_count()
    return data


def _init_process_stats() -> None:
    import psutil

    _process_stats.proc = psutil.Process()  # type: ignore[attr-defined]
    _process_stats.proc.cpu_percent(interval=None)  # první volání vrací 0, „natáhne“ měření


_init_process_stats()


@router.get("/server")
async def get_server(
    request: Request, ctx: ServerContext = Depends(get_ctx), _: Principal = Depends(require("admin")),
):
    """Podrobný stav serveru pro správce."""
    import platform
    from importlib.metadata import PackageNotFoundError, version

    def pkg(name: str) -> Optional[str]:
        try:
            return version(name)
        except PackageNotFoundError:
            return None

    st = ctx.companion.status
    c = ctx.cfg.companion
    if c.transport == "tcp":
        address = f"{c.tcp.host}:{c.tcp.port}"
    elif c.transport == "serial":
        address = c.serial.port
    else:
        address = c.ble.address or "(automatické hledání)"

    now = utcnow()
    db_stats = await ctx.db.stats()
    db_stats["events_retention_days"] = ctx.cfg.database.events_retention_days

    return serialize.to_jsonable({
        "server": {
            "version": __version__,
            "meshcore_lib": pkg("meshcore"),
            "python": platform.python_version(),
            "platform": platform.platform(terse=True),
            "started_at": _ts(ctx.started_at),
            "uptime": int(time.time() - ctx.started_at),
        },
        "database": db_stats,
        "load": {
            **_process_stats(),
            "queue_pending": await ctx.repo.count_pending_commands(),
            "messages_last_hour": await ctx.repo.count_messages_since(now - timedelta(hours=1)),
            "messages_last_24h": await ctx.repo.count_messages_since(now - timedelta(hours=24)),
            "events_last_seq": ctx.bus.last_seq,
        },
        "companion": {
            "transport": c.transport,
            "address": address,
            "state": st.state.value,
            "connected_since": _ts(st.connected_since),
            "reconnects": st.reconnects,
            "last_error": st.last_error,
            "last_rx_at": _ts(st.last_rx_at),
            "last_tx_at": _ts(st.last_tx_at),
            "last_command_ms": st.last_command_ms,
            "commands_total": st.commands_total,
            "name": st.self_info.get("name"),
            "firmware": st.device_info.get("ver"),
            "model": st.device_info.get("model"),
        },
        "clients": {
            # Za reverse proxy je to IP z X-Forwarded-For (jen od server.trusted_proxies).
            "request_ip": request.client.host if request.client else None,
            "websocket": ctx.bus.clients(),
        },
    })


# ------------------------------------------------------- export a import


@router.get("/export")
async def export_data(ctx: ServerContext = Depends(get_ctx), _: Principal = Depends(require("admin"))):
    """Kontakty a kanály v JSON (včetně klíčů kanálů – citlivé!)."""
    from ..transfer import build_export

    return await build_export(ctx.repo)


@router.post("/import")
async def import_data(
    body: dict,
    channels: Literal["skip", "regions", "device"] = Query(
        "regions",
        description="skip = kanály ignorovat; regions = jen regiony existujících kanálů; "
                    "device = zapsat kanály do companionu (přepíše sloty!)",
    ),
    ctx: ServerContext = Depends(get_ctx),
    principal: Principal = Depends(require("admin")),
):
    """Import souboru z /export. Kontakty se přidají do DB (do companionu až při potřebě)."""
    from pydantic import ValidationError

    from ..transfer import ExportFile, apply_import

    try:
        data = ExportFile.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(422, f"neplatný soubor exportu: {exc.errors()[0]['msg']}")
    try:
        result = await apply_import(ctx.repo, ctx.actions, data, channels, principal.token_id)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    if ctx.hops is not None:
        ctx.hops.invalidate()
    await ctx.bus.publish("contacts.synced", {"imported": result["contacts_added"]})
    return result
