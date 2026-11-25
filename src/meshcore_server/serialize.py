"""Převod řádků z DB na JSON pro API a události."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional


def to_jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return value


def split_hops(path: Optional[str], path_len: Optional[int]) -> Optional[list[str]]:
    """Rozdělí trasu (hex) na jednotlivé hopy.

    Velikost hashe hopu (1–3 bajty) se liší podle odesílatele; dopočítá se
    z délky trasy a počtu hopů.
    """
    if not path or not path_len or path_len <= 0 or path_len >= 255:
        return None
    raw_len = len(path) // 2
    if raw_len % path_len:
        return None
    size = raw_len // path_len * 2
    return [path[i:i + size] for i in range(0, len(path), size)]


def message(row: dict, hops: Optional[list[dict]] = None) -> dict:
    """hops: trasa s doplněnými jmény (HopResolver); bez ní jen hashe."""
    if hops is None:
        plain = split_hops(row.get("path"), row.get("path_len"))
        hops = [{"hash": h, "names": []} for h in plain] if plain else None
    data = {
        "id": row["id"],
        "kind": row["kind"],
        "direction": row["direction"],
        "contact_prefix": row.get("contact_prefix"),
        "contact_key": row.get("contact_key"),
        "author_prefix": row.get("author_prefix"),
        "channel_idx": row.get("channel_idx"),
        "sender_name": row.get("sender_name"),
        "text": row["text"],
        "txt_type": row.get("txt_type"),
        "sender_ts": row.get("sender_ts"),
        "received_at": row["received_at"],
        "path_len": row.get("path_len"),
        "path": row.get("path"),
        "hops": hops,
        "snr": row.get("snr"),
        "rssi": row.get("rssi"),
        "status": row["status"],
        "expected_ack": row.get("expected_ack"),
        "attempts": row.get("attempts"),
        "command_id": row.get("command_id"),
        "expires_at": row.get("expires_at"),
        "error": row.get("error"),
    }
    if row["direction"] == "out":
        # Opakování repeatery sledujeme jen u odeslaných zpráv.
        data["heard_repeats"] = row.get("heard_repeats") or 0
    return to_jsonable(data)


async def heard_by(row: dict, resolver) -> list[dict]:
    """Repeatery, od kterých companion zaslechl opakování odeslané zprávy."""
    hashes = [h for h in (row.get("heard_by") or "").split(",") if h]
    if not hashes:
        return []
    if resolver is None:
        return [{"hash": h, "names": []} for h in hashes]
    return await resolver.resolve(hashes) or []


async def message_with_hops(row: dict, resolver) -> dict:
    hops = await resolver.resolve(split_hops(row.get("path"), row.get("path_len"))) if resolver else None
    data = message(row, hops)
    if row.get("direction") == "out":
        data["heard_by"] = await heard_by(row, resolver)
    return data


def contact(row: dict) -> dict:
    return to_jsonable({
        "public_key": row["public_key"],
        "prefix": row["public_key"][:12],
        "name": row.get("name"),
        "type": row.get("type"),
        "flags": row.get("flags"),
        "out_path": row.get("out_path"),
        "out_path_len": row.get("out_path_len"),
        "lat": row.get("lat"),
        "lon": row.get("lon"),
        "last_advert_at": row.get("last_advert_at"),
        "last_message_at": row.get("last_message_at"),
        "favorite": bool(row.get("favorite")),
        "on_device": bool(row.get("on_device")),
        "region": row.get("region"),
        "first_seen_at": row.get("first_seen_at"),
        "updated_at": row.get("updated_at"),
    })


def login(key: str, row: Optional[dict]) -> dict:
    """Stav přihlášení k uzlu. Heslo se nikdy nevrací."""
    row = row or {}
    return to_jsonable({
        "public_key": key,
        "logged_in": bool(row.get("logged_in")),
        "is_admin": bool(row.get("is_admin")),
        "permissions": row.get("permissions"),
        "auto_login": bool(row.get("auto_login")),
        "password_saved": row.get("password") is not None,
        "last_login_at": row.get("last_login_at"),
        "last_error": row.get("last_error"),
    })


def channel(row: dict) -> dict:
    # Klíč kanálu do API neposíláme; kdo ho potřebuje, má export (admin).
    return to_jsonable({
        "idx": row["idx"], "name": row["name"], "region": row.get("region"), "updated_at": row.get("updated_at"),
    })


def command(row: dict) -> dict:
    return to_jsonable({k: row.get(k) for k in (
        "id", "type", "payload", "status", "error", "created_at", "started_at", "finished_at"
    )})


def advert(row: dict) -> dict:
    return to_jsonable({k: row.get(k) for k in (
        "id", "public_key", "name", "type", "lat", "lon", "path_len", "path", "snr", "received_at"
    )})


def device(row: Optional[dict], config: dict) -> Optional[dict]:
    if row is None:
        return None
    data = {k: v for k, v in row.items() if k != "id"}
    data["config"] = config
    return to_jsonable(data)
