"""Export a import kontaktů a kanálů (JSON).

Export obsahuje i klíče kanálů – je to citlivý soubor, nesdílejte ho.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal, Optional

from pydantic import BaseModel, Field

from . import __version__
from .db.repo import Repository
from .db.types import utcnow
from .serialize import to_jsonable

if TYPE_CHECKING:
    from .companion.actions import DeviceActions

logger = logging.getLogger("meshcore_server.transfer")

EXPORT_FORMAT = "meshcore-server-export"
EXPORT_VERSION = 1


class ExportContact(BaseModel):
    public_key: str = Field(pattern="^[0-9a-fA-F]{64}$")
    name: Optional[str] = None
    type: Optional[int] = None
    flags: Optional[int] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    favorite: bool = False
    on_device: bool = False


class ExportChannel(BaseModel):
    idx: int = Field(ge=0, le=255)
    name: str = Field(min_length=1, max_length=64)
    secret: Optional[str] = Field(None, pattern="^[0-9a-fA-F]{32}$")
    region: Optional[str] = None


class ExportFile(BaseModel):
    format: Literal["meshcore-server-export"]
    version: int
    contacts: list[ExportContact] = []
    channels: list[ExportChannel] = []


async def build_export(repo: Repository) -> dict:
    data = await repo.export_data()
    device = await repo.get_device() or {}
    return to_jsonable({
        "format": EXPORT_FORMAT,
        "version": EXPORT_VERSION,
        "exported_at": utcnow(),
        "server_version": __version__,
        "device": {"name": device.get("name"), "public_key": device.get("public_key")},
        "contacts": [
            {k: c.get(k) for k in ("public_key", "name", "type", "flags", "lat", "lon")}
            | {"favorite": bool(c.get("favorite")), "on_device": bool(c.get("on_device"))}
            for c in data["contacts"]
        ],
        "channels": [
            {"idx": c["idx"], "name": c["name"], "secret": c.get("secret"), "region": c.get("region")}
            for c in data["channels"]
        ],
    })


async def apply_import(
    repo: Repository,
    actions: Optional["DeviceActions"],
    data: ExportFile,
    channels_mode: Literal["skip", "regions", "device"] = "regions",
    token_id: Optional[int] = None,
) -> dict:
    """Importuje kontakty do DB a podle channels_mode kanály.

    channels_mode:
      skip    – kanály ignorovat
      regions – jen převzít regiony kanálů, které v companionu už jsou (stejné číslo i jméno)
      device  – zapsat kanály do companionu (přepíše dané sloty!)
    """
    if data.version != EXPORT_VERSION:
        raise ValueError(f"nepodporovaná verze exportu {data.version}")

    added, updated = await repo.import_contacts([c.model_dump() for c in data.contacts])
    result = {"contacts_added": added, "contacts_updated": updated, "channels": 0, "channels_errors": []}

    if channels_mode == "regions":
        current = {c["idx"]: c for c in await repo.list_channels()}
        for ch in data.channels:
            if ch.region and ch.idx in current and current[ch.idx]["name"] == ch.name:
                await repo.set_channel_region(ch.idx, ch.region)
                result["channels"] += 1
    elif channels_mode == "device":
        if actions is None:
            raise ValueError("zápis kanálů do companionu vyžaduje běžící server (použijte API)")
        for ch in data.channels:
            secret = None if ch.name.startswith("#") else ch.secret
            if not ch.name.startswith("#") and not secret:
                result["channels_errors"].append(f"{ch.idx}: soukromý kanál bez klíče")
                continue
            try:
                await actions.set_channel(ch.idx, ch.name, secret, ch.region, token_id)
                result["channels"] += 1
            except Exception as exc:  # noqa: BLE001 – pokračovat s dalšími kanály
                result["channels_errors"].append(f"{ch.idx} ({ch.name}): {exc}")

    logger.info("Import: %s", result)
    return result
