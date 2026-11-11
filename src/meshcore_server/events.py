"""Event bus: události pro klienty (WebSocket).

Každá událost se nejdřív uloží do tabulky events (dostane pořadové číslo seq)
a teprve pak se rozešle připojeným klientům. Klient, kterému spadlo spojení,
si po připojení s ?since=<seq> vyžádá, co mezitím zmeškal.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from .db.repo import Repository
from .db.types import utcnow
from .serialize import to_jsonable

logger = logging.getLogger("meshcore_server.events")

SUBSCRIBER_QUEUE_SIZE = 1000


class Subscriber:
    def __init__(self, info: Optional[dict] = None) -> None:
        self.queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=SUBSCRIBER_QUEUE_SIZE)
        self.overflowed = False
        self.info = info or {}  # ip, token, connected_at – pro /server


class EventBus:
    def __init__(self, repo: Repository) -> None:
        self.repo = repo
        self._subscribers: set[Subscriber] = set()
        self._lock = asyncio.Lock()  # zachová pořadí seq i při souběžném publikování
        self.last_seq: int = 0

    async def init(self) -> None:
        _, max_seq = await self.repo.event_seq_bounds()
        self.last_seq = max_seq or 0

    async def publish(self, type_: str, data: Any = None) -> Optional[dict]:
        data = to_jsonable(data)
        async with self._lock:
            now = utcnow()
            try:
                seq = await self.repo.insert_event(type_, data, now)
            except Exception:  # noqa: BLE001 – výpadek DB nesmí shodit příjem dat
                logger.exception("Uložení události %s selhalo", type_)
                return None
            self.last_seq = seq
            envelope = {"seq": seq, "type": type_, "ts": to_jsonable(now), "data": data}
            for sub in list(self._subscribers):
                try:
                    sub.queue.put_nowait(envelope)
                except asyncio.QueueFull:
                    # Klient nestíhá – odpojíme ho, po reconnectu si zmeškané dotáhne.
                    sub.overflowed = True
                    self._subscribers.discard(sub)
            return envelope

    def subscribe(self, info: Optional[dict] = None) -> Subscriber:
        sub = Subscriber(info)
        self._subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: Subscriber) -> None:
        self._subscribers.discard(sub)

    def clients(self) -> list[dict]:
        return [dict(sub.info, queued=sub.queue.qsize()) for sub in self._subscribers]

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)
