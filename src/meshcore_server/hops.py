"""Překlad hashů hopů v trase na jména repeaterů.

Hash hopu je začátek (1–3 bajty) public key repeateru, přes který zpráva
prošla. Jména hledáme v kontaktech; u krátkých hashů může odpovídat víc
repeaterů, proto se vrací seznam.
"""

from __future__ import annotations

import time
from typing import Optional

from .db.repo import Repository

# Typy kontaktů, které přeposílají pakety: 2 = repeater, 3 = room server.
RELAY_TYPES = (2, 3)
CACHE_TTL = 60.0  # s


class HopResolver:
    def __init__(self, repo: Repository) -> None:
        self.repo = repo
        self._relays: list[tuple[str, str]] = []  # (public_key, jméno)
        self._loaded_at: Optional[float] = None

    def invalidate(self) -> None:
        self._loaded_at = None

    async def _ensure_loaded(self) -> None:
        if self._loaded_at is not None and time.monotonic() - self._loaded_at < CACHE_TTL:
            return
        relays = []
        for type_ in RELAY_TYPES:
            for c in await self.repo.list_contacts(type_=type_):
                relays.append((c["public_key"].lower(), c.get("name") or c["public_key"][:12]))
        self._relays = relays
        self._loaded_at = time.monotonic()

    async def resolve(self, hops: Optional[list[str]]) -> Optional[list[dict]]:
        if not hops:
            return None
        await self._ensure_loaded()
        result = []
        for h in hops:
            h = h.lower()
            names = sorted({name for key, name in self._relays if key.startswith(h)})
            result.append({"hash": h, "names": names})
        return result
