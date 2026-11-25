"""Region (flood scope) pro jednotlivé odeslání.

Companion má jeden aktuální region, který platí pro všechny pakety odeslané floodem.
Kanály a kontakty si můžou nastavit vlastní: před odesláním se nastaví, po něm se vrátí
výchozí region zařízení. Pakety se známou cestou (direct) region nepoužívají.

Hodnoty: None = výchozí region zařízení, "*" = bez regionu (všude), "#jmeno" = region.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional

from meshcore import EventType

MAX_REGION_BYTES = 31


def normalize_region(value: Optional[str]) -> Optional[str]:
    """Hodnota z API -> uložená podoba. Vyhodí ValueError u neplatné."""
    region = (value or "").strip()
    if not region:
        return None
    if region == "*":
        return "*"
    if not region.startswith("#"):
        region = "#" + region
    if len(region.encode("utf-8")) > MAX_REGION_BYTES:
        raise ValueError(f"region může mít nejvýš {MAX_REGION_BYTES} bajtů")
    return region


class ScopeError(Exception):
    pass


async def with_scope(mc, region: Optional[str], fn: Callable[[], Awaitable[Any]]) -> Any:
    """Provede fn() s nastaveným regionem; volá se pod zámkem spojení (manager.call)."""
    if not region:
        return await fn()
    res = await mc.commands.set_flood_scope(region)
    if res.type == EventType.ERROR:
        raise ScopeError(f"nastavení regionu {region} selhalo: {res.payload}")
    try:
        return await fn()
    finally:
        # vrátit výchozí region, ať neplatí pro další zprávy
        await mc.commands.set_flood_scope(None)
