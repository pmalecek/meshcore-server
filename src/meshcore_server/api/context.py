from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..auth import TokenService
    from ..companion import CompanionManager
    from ..companion.actions import DeviceActions
    from ..companion.queue import CommandQueue
    from ..companion.tracker import NodeTracker
    from ..config import Config
    from ..db import Database
    from ..db.repo import Repository
    from ..events import EventBus
    from ..hops import HopResolver


@dataclass
class ServerContext:
    """Vše, co API potřebuje; vytváří ho Application."""

    cfg: "Config"
    db: "Database"
    repo: "Repository"
    bus: "EventBus"
    tokens: "TokenService"
    companion: "CompanionManager"
    hops: "HopResolver"
    queue: "CommandQueue"
    actions: "DeviceActions"
    tracker: "NodeTracker | None" = None
    started_at: float = field(default_factory=time.time)
