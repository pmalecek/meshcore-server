"""API tokeny a krátkodobé lístky pro WebSocket."""

from __future__ import annotations

import hashlib
import secrets
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from .db.repo import Repository
from .db.types import utcnow

SCOPES = ("read", "write", "admin")
TOKEN_PREFIX = "mcs_"
TICKET_TTL = 60.0          # s
TOUCH_INTERVAL = timedelta(minutes=1)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def scope_allows(granted: str, required: str) -> bool:
    """admin ⊃ write ⊃ read."""
    return SCOPES.index(granted) >= SCOPES.index(required)


@dataclass
class Principal:
    token_id: int
    name: str
    scope: str

    def allows(self, required: str) -> bool:
        return scope_allows(self.scope, required)


class TokenService:
    def __init__(self, repo: Repository) -> None:
        self.repo = repo
        self._tickets: dict[str, tuple[float, Principal]] = {}
        self._last_touch: dict[int, object] = {}

    # ---------------------------------------------------------- správa (CLI)

    async def create(self, name: str, scope: str) -> str:
        if scope not in SCOPES:
            raise ValueError(f"neznámé oprávnění {scope!r}, povolené: {', '.join(SCOPES)}")
        token = TOKEN_PREFIX + secrets.token_urlsafe(32)
        await self.repo.create_token(name, hash_token(token), scope)
        return token

    # ------------------------------------------------------------ ověření

    async def authenticate(self, token: Optional[str]) -> Optional[Principal]:
        if not token:
            return None
        row = await self.repo.get_token_by_hash(hash_token(token))
        if row is None:
            return None
        now = utcnow()
        last = row.get("last_used_at")
        if last is None or now - last > TOUCH_INTERVAL:
            await self.repo.touch_token(row["id"], now)
        return Principal(token_id=row["id"], name=row["name"], scope=row["scopes"])

    def issue_ticket(self, principal: Principal) -> str:
        self._expire_tickets()
        ticket = secrets.token_urlsafe(24)
        self._tickets[ticket] = (time.monotonic() + TICKET_TTL, principal)
        return ticket

    def redeem_ticket(self, ticket: Optional[str]) -> Optional[Principal]:
        """Lístek platí jednou a krátce (pro WebSocket z prohlížeče)."""
        self._expire_tickets()
        if not ticket:
            return None
        entry = self._tickets.pop(ticket, None)
        return entry[1] if entry else None

    def _expire_tickets(self) -> None:
        now = time.monotonic()
        for key in [k for k, (exp, _) in self._tickets.items() if exp < now]:
            del self._tickets[key]
