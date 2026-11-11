"""Výpis událostí z companionu do logu (krok 1 – ověření komunikace).

V dalších krocích sem místo logu napojíme ukládání do DB a event bus.
"""

from __future__ import annotations

import logging
from typing import Any

from meshcore import EventType

logger = logging.getLogger("meshcore_server.events")

# Události, které chodí často a pro běžný provoz nejsou zajímavé.
NOISY_EVENTS = {
    EventType.RX_LOG_DATA,
    EventType.LOG_DATA,
    EventType.RAW_DATA,
    EventType.OK,
    EventType.NO_MORE_MSGS,
    EventType.CURRENT_TIME,
    EventType.BATTERY,  # keepalive
    EventType.NEXT_CONTACT,  # po jednom při načítání seznamu kontaktů
    EventType.CHANNEL_INFO,  # při načítání kanálů (shrnutí vypisuje manager)
    EventType.SELF_INFO,
    EventType.DEVICE_INFO,
    EventType.DEFAULT_FLOOD_SCOPE,  # při synchronizaci (shrnutí vypisuje manager)
}

_MAX_VALUE_LEN = 80


def _short(value: Any) -> str:
    text = str(value)
    return text if len(text) <= _MAX_VALUE_LEN else text[: _MAX_VALUE_LEN - 1] + "…"


def format_event(event_type: EventType, payload: Any) -> str:
    """Krátký jednořádkový popis události."""
    p = payload if isinstance(payload, dict) else {}

    if event_type == EventType.CONTACT_MSG_RECV:
        return (
            f"DM od {p.get('pubkey_prefix', '?')} "
            f"(hopy {p.get('path_len', '?')}, SNR {p.get('SNR', '?')}): {p.get('text', '')!r}"
        )
    if event_type == EventType.CHANNEL_MSG_RECV:
        return (
            f"kanál #{p.get('channel_idx', '?')} "
            f"(hopy {p.get('path_len', '?')}, SNR {p.get('SNR', '?')}): {p.get('text', '')!r}"
        )
    if event_type == EventType.ADVERTISEMENT:
        return f"advert od {_short(p.get('public_key', '?'))}"
    if event_type == EventType.NEW_CONTACT:
        return f"nový kontakt {p.get('adv_name', '?')} ({_short(p.get('public_key', '?'))})"
    if event_type == EventType.PATH_UPDATE:
        return f"aktualizace trasy ke {_short(p.get('public_key', '?'))}"
    if event_type == EventType.ACK:
        return f"ACK {p.get('code', '?')}"
    if event_type == EventType.MSG_SENT:
        return f"zpráva odeslána, očekávaný ACK {p.get('expected_ack', '?')}"
    if event_type == EventType.CONTACTS:
        count = len(payload) if isinstance(payload, dict) else "?"
        return f"seznam kontaktů ({count})"
    if event_type in (EventType.CONNECTED, EventType.DISCONNECTED, EventType.ERROR):
        return _short(p)
    if event_type == EventType.MESSAGES_WAITING:
        return "companion má čekající zprávy"

    if isinstance(payload, dict):
        items = ", ".join(f"{k}={_short(v)}" for k, v in list(payload.items())[:6])
        return items
    return _short(payload)


class EventLogger:
    def __init__(self, include_noisy: bool = False) -> None:
        self.include_noisy = include_noisy

    async def __call__(self, event) -> None:
        if event.type in NOISY_EVENTS and not self.include_noisy:
            return
        logger.info("[%s] %s", event.type.value, format_event(event.type, event.payload))
