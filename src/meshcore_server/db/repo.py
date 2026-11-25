"""Zápisy a čtení dat – jediné místo, kde se skládají SQL dotazy.

Upsert a „insert ignore“ se liší mezi SQLite a MySQL; řeší to _upsert a
_insert_ignore podle dialektu.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Optional

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects import mysql, sqlite
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from . import schema as s
from .types import utcnow


def _dialect(conn: AsyncConnection) -> str:
    return conn.dialect.name


async def _upsert(conn: AsyncConnection, table, rows: list[dict], keys: list[str], update_cols: Optional[list[str]] = None) -> None:
    """INSERT … ON CONFLICT/DUPLICATE KEY UPDATE pro jeden nebo více řádků.

    update_cols: sloupce, které se při konfliktu přepíší (výchozí: všechny kromě klíčů).
    """
    if not rows:
        return
    cols = update_cols if update_cols is not None else [c for c in rows[0] if c not in keys]
    if _dialect(conn) == "mysql":
        stmt = mysql.insert(table)
        stmt = stmt.on_duplicate_key_update({c: stmt.inserted[c] for c in cols}) if cols else stmt.prefix_with("IGNORE")
    else:
        stmt = sqlite.insert(table)
        if cols:
            stmt = stmt.on_conflict_do_update(index_elements=keys, set_={c: stmt.excluded[c] for c in cols})
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=keys)
    await conn.execute(stmt, rows)


async def _insert_ignore(conn: AsyncConnection, table, row: dict) -> bool:
    """Vloží řádek, pokud neporuší unikátní klíč. Vrátí True, když se vložil."""
    if _dialect(conn) == "mysql":
        stmt = mysql.insert(table).prefix_with("IGNORE")
    else:
        stmt = sqlite.insert(table).on_conflict_do_nothing()
    result = await conn.execute(stmt, row)
    return (result.rowcount or 0) > 0


class Repository:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    # ------------------------------------------------------------- device

    async def get_device(self) -> Optional[dict]:
        async with self.engine.connect() as conn:
            row = (await conn.execute(select(s.device).where(s.device.c.id == 1))).mappings().first()
            return dict(row) if row else None

    async def save_device(self, values: dict, replace_identity: bool = False) -> None:
        """Uloží companion (řádek id=1). first_seen_at se při změně identity přepíše."""
        now = utcnow()
        async with self.engine.begin() as conn:
            existing = (await conn.execute(select(s.device.c.first_seen_at).where(s.device.c.id == 1))).first()
            row = {**values, "id": 1, "updated_at": now, "last_connected_at": now}
            row["first_seen_at"] = now if (existing is None or replace_identity) else existing.first_seen_at
            await _upsert(conn, s.device, [row], ["id"])

    async def save_device_config(self, values: dict[str, Any]) -> None:
        now = utcnow()
        rows = [{"key": k, "value": v, "updated_at": now} for k, v in values.items()]
        async with self.engine.begin() as conn:
            await _upsert(conn, s.device_config, rows, ["key"])

    # ----------------------------------------------------------- contacts

    @staticmethod
    def _contact_row(c: dict, on_device: bool, now: datetime) -> dict:
        last_advert = c.get("last_advert") or 0
        out_len = c.get("out_path_len")
        return {
            "public_key": c["public_key"],
            "name": (c.get("adv_name") or "")[:64] or None,
            "type": c.get("type"),
            "flags": c.get("flags"),
            "out_path": c.get("out_path") or None,
            "out_path_len": out_len,
            "lat": c.get("adv_lat") or None,
            "lon": c.get("adv_lon") or None,
            "last_advert_at": datetime.fromtimestamp(last_advert, tz=now.tzinfo) if last_advert else None,
            "on_device": on_device,
            "updated_at": now,
            # first_seen_at a favorite se nastaví jen u nového kontaktu (viz níže)
            "first_seen_at": now,
            "favorite": False,
        }

    async def upsert_contacts(self, contacts: Iterable[dict], on_device: bool = True) -> int:
        now = utcnow()
        rows = [self._contact_row(c, on_device, now) for c in contacts if c.get("public_key")]
        update_cols = [c for c in rows[0] if c not in ("public_key", "first_seen_at", "favorite")] if rows else []
        async with self.engine.begin() as conn:
            await _upsert(conn, s.contacts, rows, ["public_key"], update_cols)
        return len(rows)

    async def mark_contacts_off_device(self, keep_keys: Iterable[str]) -> int:
        """Kontakty, které v companionu nejsou, označí on_device = 0 (v DB zůstanou)."""
        keep = list(keep_keys)
        stmt = update(s.contacts).where(s.contacts.c.on_device.is_(True))
        if keep:
            stmt = stmt.where(s.contacts.c.public_key.not_in(keep))
        async with self.engine.begin() as conn:
            result = await conn.execute(stmt.values(on_device=False, updated_at=utcnow()))
            return result.rowcount or 0

    async def set_contact_off_device(self, public_key: str) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                update(s.contacts).where(s.contacts.c.public_key == public_key)
                .values(on_device=False, updated_at=utcnow())
            )

    async def find_contact_by_prefix(self, prefix: str) -> Optional[dict]:
        async with self.engine.connect() as conn:
            row = (await conn.execute(
                select(s.contacts).where(s.contacts.c.public_key.startswith(prefix.lower())).limit(1)
            )).mappings().first()
            return dict(row) if row else None

    async def get_contact(self, public_key: str) -> Optional[dict]:
        async with self.engine.connect() as conn:
            row = (await conn.execute(
                select(s.contacts).where(s.contacts.c.public_key == public_key)
            )).mappings().first()
            return dict(row) if row else None

    async def touch_contact_message(self, public_key: str, when: datetime) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                update(s.contacts).where(s.contacts.c.public_key == public_key).values(last_message_at=when)
            )

    # ----------------------------------------------------------- channels

    async def replace_channels(self, channels: list[dict]) -> None:
        """Kanály v DB odpovídají companionu: nastavené se uloží, prázdné sloty smažou."""
        now = utcnow()
        rows = [
            {"idx": c["idx"], "name": c["name"][:64], "secret": c.get("secret"), "updated_at": now}
            for c in channels
        ]
        async with self.engine.begin() as conn:
            await _upsert(conn, s.channels, rows, ["idx"])
            stmt = delete(s.channels)
            if rows:
                stmt = stmt.where(s.channels.c.idx.not_in([r["idx"] for r in rows]))
            await conn.execute(stmt)

    # ----------------------------------------------------- messages/adverts

    async def insert_message(self, row: dict) -> Optional[int]:
        """Vloží zprávu; při duplicitě (stejný dedup_hash) vrátí None."""
        async with self.engine.begin() as conn:
            if not await _insert_ignore(conn, s.messages, row):
                return None
            msg_id = (await conn.execute(
                select(s.messages.c.id).where(s.messages.c.dedup_hash == row["dedup_hash"])
            )).scalar_one()
            return msg_id

    async def insert_advert(self, row: dict) -> int:
        async with self.engine.begin() as conn:
            result = await conn.execute(s.adverts.insert(), row)
            return int(result.inserted_primary_key[0])

    # ------------------------------------------------------------- čtení

    async def get_message(self, msg_id: int) -> Optional[dict]:
        async with self.engine.connect() as conn:
            row = (await conn.execute(select(s.messages).where(s.messages.c.id == msg_id))).mappings().first()
            return dict(row) if row else None

    async def list_messages(
        self,
        kind: Optional[str] = None,
        contact_prefix: Optional[str] = None,
        channel_idx: Optional[int] = None,
        before: Optional[int] = None,
        after: Optional[int] = None,
        limit: int = 50,
    ) -> list[dict]:
        """Zprávy od nejnovější. before/after = id zprávy (kurzor)."""
        m = s.messages
        stmt = select(m)
        if kind:
            stmt = stmt.where(m.c.kind == kind)
        if contact_prefix:
            stmt = stmt.where(m.c.contact_prefix == contact_prefix.lower()[:12])
        if channel_idx is not None:
            stmt = stmt.where(m.c.channel_idx == channel_idx)
        if before is not None:
            stmt = stmt.where(m.c.id < before)
        if after is not None:
            # novější než kurzor: bereme nejstarší nad kurzorem a pak otočíme
            stmt = stmt.where(m.c.id > after).order_by(m.c.id.asc()).limit(limit)
            async with self.engine.connect() as conn:
                rows = [dict(r) for r in (await conn.execute(stmt)).mappings()]
            return list(reversed(rows))
        stmt = stmt.order_by(m.c.id.desc()).limit(limit)
        async with self.engine.connect() as conn:
            return [dict(r) for r in (await conn.execute(stmt)).mappings()]

    async def list_contacts(
        self, type_: Optional[int] = None, on_device: Optional[bool] = None, q: Optional[str] = None
    ) -> list[dict]:
        c = s.contacts
        stmt = select(c)
        if type_ is not None:
            stmt = stmt.where(c.c.type == type_)
        if on_device is not None:
            stmt = stmt.where(c.c.on_device.is_(on_device))
        if q:
            stmt = stmt.where(c.c.name.contains(q, autoescape=True))
        stmt = stmt.order_by(c.c.name)
        async with self.engine.connect() as conn:
            return [dict(r) for r in (await conn.execute(stmt)).mappings()]

    async def get_contacts_by_keys(self, keys: Iterable[str]) -> list[dict]:
        keys = list(keys)
        if not keys:
            return []
        async with self.engine.connect() as conn:
            rows = (await conn.execute(select(s.contacts).where(s.contacts.c.public_key.in_(keys)))).mappings()
            return [dict(r) for r in rows]

    async def list_channels(self) -> list[dict]:
        async with self.engine.connect() as conn:
            rows = (await conn.execute(select(s.channels).order_by(s.channels.c.idx))).mappings()
            return [dict(r) for r in rows]

    async def list_adverts(
        self, public_key: Optional[str] = None, before: Optional[int] = None, limit: int = 50
    ) -> list[dict]:
        a = s.adverts
        stmt = select(a)
        if public_key:
            stmt = stmt.where(a.c.public_key.startswith(public_key.lower()))
        if before is not None:
            stmt = stmt.where(a.c.id < before)
        stmt = stmt.order_by(a.c.id.desc()).limit(limit)
        async with self.engine.connect() as conn:
            return [dict(r) for r in (await conn.execute(stmt)).mappings()]

    async def get_device_config(self) -> dict[str, Any]:
        async with self.engine.connect() as conn:
            rows = (await conn.execute(select(s.device_config))).mappings()
            return {r["key"]: r["value"] for r in rows}

    # ------------------------------------------------------------- events

    async def insert_event(self, type_: str, data: Any, created_at: datetime) -> int:
        async with self.engine.begin() as conn:
            result = await conn.execute(s.events.insert().values(type=type_, data=data, created_at=created_at))
            return int(result.inserted_primary_key[0])

    async def events_after(self, seq: int, limit: int = 1000) -> list[dict]:
        e = s.events
        async with self.engine.connect() as conn:
            rows = (await conn.execute(
                select(e).where(e.c.seq > seq).order_by(e.c.seq.asc()).limit(limit)
            )).mappings()
            return [dict(r) for r in rows]

    async def event_seq_bounds(self) -> tuple[Optional[int], Optional[int]]:
        e = s.events
        async with self.engine.connect() as conn:
            row = (await conn.execute(select(func.min(e.c.seq), func.max(e.c.seq)))).first()
            return (row[0], row[1]) if row else (None, None)

    async def prune_events(self, older_than: datetime) -> int:
        async with self.engine.begin() as conn:
            result = await conn.execute(delete(s.events).where(s.events.c.created_at < older_than))
            return result.rowcount or 0

    # ------------------------------------------------------------- tokeny

    async def create_token(self, name: str, token_hash: str, scopes: str) -> int:
        async with self.engine.begin() as conn:
            result = await conn.execute(s.api_tokens.insert().values(
                name=name, token_hash=token_hash, scopes=scopes, created_at=utcnow()
            ))
            return int(result.inserted_primary_key[0])

    async def get_token_by_hash(self, token_hash: str) -> Optional[dict]:
        t = s.api_tokens
        async with self.engine.connect() as conn:
            row = (await conn.execute(
                select(t).where(t.c.token_hash == token_hash, t.c.revoked_at.is_(None))
            )).mappings().first()
            return dict(row) if row else None

    async def touch_token(self, token_id: int, when: datetime) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(update(s.api_tokens).where(s.api_tokens.c.id == token_id).values(last_used_at=when))

    async def list_tokens(self) -> list[dict]:
        t = s.api_tokens
        async with self.engine.connect() as conn:
            rows = (await conn.execute(select(t).order_by(t.c.id))).mappings()
            return [dict(r) for r in rows]

    async def revoke_token(self, name: str) -> bool:
        t = s.api_tokens
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(t).where(t.c.name == name, t.c.revoked_at.is_(None)).values(revoked_at=utcnow())
            )
            return (result.rowcount or 0) > 0

    async def delete_token(self, name: str) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(delete(s.api_tokens).where(s.api_tokens.c.name == name))

    # ------------------------------------------------- odchozí zprávy a fronta

    async def get_channel(self, idx: int) -> Optional[dict]:
        async with self.engine.connect() as conn:
            row = (await conn.execute(select(s.channels).where(s.channels.c.idx == idx))).mappings().first()
            return dict(row) if row else None

    async def set_contact_region(self, public_key: str, region: Optional[str]) -> bool:
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(s.contacts).where(s.contacts.c.public_key == public_key).values(region=region, updated_at=utcnow())
            )
            return (result.rowcount or 0) > 0

    async def set_channel_region(self, idx: int, region: Optional[str]) -> bool:
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(s.channels).where(s.channels.c.idx == idx).values(region=region, updated_at=utcnow())
            )
            return (result.rowcount or 0) > 0

    async def create_outgoing_message(self, message: dict, token_id: Optional[int]) -> tuple[int, int]:
        """Uloží odchozí zprávu a příkaz k jejímu odeslání v jedné transakci."""
        now = utcnow()
        async with self.engine.begin() as conn:
            msg_id = int((await conn.execute(s.messages.insert().values(**message))).inserted_primary_key[0])
            cmd_id = int((await conn.execute(s.commands.insert().values(
                type="send_msg", payload={"message_id": msg_id}, status="pending",
                token_id=token_id, created_at=now,
            ))).inserted_primary_key[0])
            await conn.execute(update(s.messages).where(s.messages.c.id == msg_id).values(command_id=cmd_id))
            return msg_id, cmd_id

    async def insert_command(self, type_: str, payload: dict, token_id: Optional[int] = None) -> int:
        async with self.engine.begin() as conn:
            result = await conn.execute(s.commands.insert().values(
                type=type_, payload=payload, status="pending", token_id=token_id, created_at=utcnow(),
            ))
            return int(result.inserted_primary_key[0])

    async def get_command(self, cmd_id: int) -> Optional[dict]:
        async with self.engine.connect() as conn:
            row = (await conn.execute(select(s.commands).where(s.commands.c.id == cmd_id))).mappings().first()
            return dict(row) if row else None

    async def next_pending_command(self) -> Optional[dict]:
        c = s.commands
        async with self.engine.connect() as conn:
            row = (await conn.execute(
                select(c).where(c.c.status == "pending").order_by(c.c.id.asc()).limit(1)
            )).mappings().first()
            return dict(row) if row else None

    async def update_command(self, cmd_id: int, **values) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(update(s.commands).where(s.commands.c.id == cmd_id).values(**values))

    async def requeue_running_commands(self) -> int:
        """Příkazy rozpracované při pádu/restartu vrátí do fronty."""
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(s.commands).where(s.commands.c.status == "running").values(status="pending", started_at=None)
            )
            return result.rowcount or 0

    async def update_message(self, msg_id: int, **values) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(update(s.messages).where(s.messages.c.id == msg_id).values(**values))

    async def find_message_by_ack(self, code: str) -> Optional[dict]:
        m = s.messages
        async with self.engine.connect() as conn:
            row = (await conn.execute(
                select(m).where(m.c.expected_ack == code, m.c.direction == "out").order_by(m.c.id.desc()).limit(1)
            )).mappings().first()
            return dict(row) if row else None

    async def expire_pending_messages(self, now: datetime) -> list[int]:
        """Odchozí zprávy, které nestihly odejít do expires_at, označí jako failed."""
        m, c = s.messages, s.commands
        async with self.engine.begin() as conn:
            ids = [r[0] for r in await conn.execute(
                select(m.c.id).where(m.c.direction == "out", m.c.status == "pending", m.c.expires_at < now)
            )]
            if ids:
                await conn.execute(update(m).where(m.c.id.in_(ids)).values(
                    status="failed", error="vypršel čas na odeslání (companion nebyl dostupný)"
                ))
                await conn.execute(update(c).where(c.c.status == "pending", c.c.type == "send_msg").where(
                    c.c.id.in_(select(m.c.command_id).where(m.c.id.in_(ids)))
                ).values(status="failed", error="expired", finished_at=now))
            return ids

    # ------------------------------------------------- správa kontaktů/kanálů

    async def update_contact_fields(self, public_key: str, **values) -> bool:
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(s.contacts).where(s.contacts.c.public_key == public_key)
                .values(updated_at=utcnow(), **values)
            )
            return (result.rowcount or 0) > 0

    async def delete_contact(self, public_key: str) -> bool:
        """Smaže kontakt z DB. Zprávy zůstanou (mají vlastní kopii klíče a jména)."""
        async with self.engine.begin() as conn:
            result = await conn.execute(delete(s.contacts).where(s.contacts.c.public_key == public_key))
            return (result.rowcount or 0) > 0

    async def count_on_device(self) -> int:
        async with self.engine.connect() as conn:
            return (await conn.execute(
                select(func.count()).select_from(s.contacts).where(s.contacts.c.on_device.is_(True))
            )).scalar_one()

    async def eviction_candidate(self, exclude: Optional[str] = None) -> Optional[dict]:
        """Kontakt, který lze z companionu odebrat: ne oblíbený, nejdéle neaktivní.

        Repeatery a room servery mají přednost v ponechání (jsou potřeba pro trasy
        a správu), odebírají se až po chat kontaktech a senzorech.
        """
        c = s.contacts
        activity = func.coalesce(c.c.last_message_at, c.c.last_advert_at, c.c.first_seen_at)
        relay_last = c.c.type.in_([2, 3])
        stmt = (
            select(c)
            .where(c.c.on_device.is_(True), c.c.favorite.is_(False))
            .order_by(relay_last.asc(), activity.asc())
            .limit(1)
        )
        if exclude:
            stmt = stmt.where(c.c.public_key != exclude)
        async with self.engine.connect() as conn:
            row = (await conn.execute(stmt)).mappings().first()
            return dict(row) if row else None

    async def save_channel(self, idx: int, name: str, secret: Optional[str]) -> None:
        """Uloží kanál z companionu; region (jen na serveru) zůstane zachován."""
        async with self.engine.begin() as conn:
            await _upsert(conn, s.channels, [
                {"idx": idx, "name": name[:64], "secret": secret, "updated_at": utcnow()}
            ], ["idx"])

    async def delete_channel(self, idx: int) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(delete(s.channels).where(s.channels.c.idx == idx))

    async def record_command(
        self, type_: str, payload: Any, status: str, error: Optional[str], token_id: Optional[int]
    ) -> int:
        """Záznam o přímo provedeném příkazu (správa), pro historii v commands."""
        now = utcnow()
        async with self.engine.begin() as conn:
            result = await conn.execute(s.commands.insert().values(
                type=type_, payload=payload, status=status, error=error, token_id=token_id,
                created_at=now, started_at=now, finished_at=now,
            ))
            return int(result.inserted_primary_key[0])

    # -------------------------------------------------------- stav přečtení

    async def get_read_state(self, reader: str) -> dict[str, int]:
        r = s.read_state
        async with self.engine.connect() as conn:
            rows = await conn.execute(select(r.c.conversation, r.c.last_read_message_id).where(r.c.reader == reader))
            return {row[0]: row[1] for row in rows}

    async def set_read_state(self, reader: str, conversation: str, message_id: int, force: bool = False) -> int:
        """Posune značku přečtení; bez force jen dopředu. Vrátí platnou hodnotu."""
        current = (await self.get_read_state(reader)).get(conversation)
        if current is not None and message_id <= current and not force:
            return current
        async with self.engine.begin() as conn:
            await _upsert(conn, s.read_state, [{
                "reader": reader, "conversation": conversation,
                "last_read_message_id": message_id, "updated_at": utcnow(),
            }], ["reader", "conversation"])
        return message_id

    async def conversations(self) -> list[dict]:
        """Konverzace s příchozími zprávami: klíč, poslední id, počet zpráv."""
        m = s.messages
        stmt = (
            select(m.c.kind, m.c.contact_prefix, m.c.channel_idx, func.max(m.c.id), func.count())
            .where(m.c.direction == "in")
            .group_by(m.c.kind, m.c.contact_prefix, m.c.channel_idx)
        )
        result = []
        async with self.engine.connect() as conn:
            for kind, prefix, idx, last_id, count in await conn.execute(stmt):
                if kind in ("dm", "room") and prefix:
                    key = f"{kind}:{prefix}"
                elif kind == "channel" and idx is not None:
                    key = f"ch:{idx}"
                else:
                    continue
                result.append({"conversation": key, "last_message_id": last_id, "total": count})
        return result

    async def count_unread(self, conversation: str, after_id: int) -> int:
        m = s.messages
        stmt = select(func.count()).select_from(m).where(m.c.direction == "in", m.c.id > after_id)
        kind, _, peer = conversation.partition(":")
        if kind in ("dm", "room"):
            stmt = stmt.where(m.c.kind == kind, m.c.contact_prefix == peer)
        else:
            stmt = stmt.where(m.c.kind == "channel", m.c.channel_idx == int(peer))
        async with self.engine.connect() as conn:
            return (await conn.execute(stmt)).scalar_one()

    # ---------------------------------------------------------- statistiky

    async def count_messages_since(self, since: datetime) -> dict[str, int]:
        m = s.messages
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                select(m.c.direction, func.count()).where(m.c.received_at >= since).group_by(m.c.direction)
            )
            counts = {d: n for d, n in rows}
        return {"in": counts.get("in", 0), "out": counts.get("out", 0)}

    async def count_pending_commands(self) -> int:
        async with self.engine.connect() as conn:
            return (await conn.execute(
                select(func.count()).select_from(s.commands).where(s.commands.c.status.in_(["pending", "running"]))
            )).scalar_one()


    # ----------------------------------------------------------- pročišťování

    async def purge(self, now: datetime, messages_days: int, adverts_days: int, commands_days: int,
                    dry_run: bool = False) -> dict[str, int]:
        """Smaže záznamy starší než zadaný počet dní (0 = tabulku nechat být)."""
        from datetime import timedelta

        m, a, c = s.messages, s.adverts, s.commands
        plan = []
        if messages_days:
            # Odchozí zprávy, které se ještě odesílají, nemažeme.
            plan.append(("messages", m, (m.c.received_at < now - timedelta(days=messages_days))
                         & m.c.status.not_in(["pending"])))
        if adverts_days:
            plan.append(("adverts", a, a.c.received_at < now - timedelta(days=adverts_days)))
        if commands_days:
            plan.append(("commands", c, (c.c.created_at < now - timedelta(days=commands_days))
                         & c.c.status.in_(["done", "failed"])))
        result = {}
        async with self.engine.begin() as conn:
            for name, table, cond in plan:
                if dry_run:
                    result[name] = (await conn.execute(select(func.count()).select_from(table).where(cond))).scalar_one()
                else:
                    result[name] = (await conn.execute(delete(table).where(cond))).rowcount or 0
        return result

    # ---------------------------------------------------------- export/import

    async def export_data(self) -> dict:
        async with self.engine.connect() as conn:
            contacts = [dict(r) for r in (await conn.execute(select(s.contacts).order_by(s.contacts.c.name))).mappings()]
            channels = [dict(r) for r in (await conn.execute(select(s.channels).order_by(s.channels.c.idx))).mappings()]
        return {"contacts": contacts, "channels": channels}

    async def import_contacts(self, contacts: list[dict]) -> tuple[int, int]:
        """Vloží kontakty, které v DB nejsou; u existujících převezme jen favorite. Vrátí (nové, aktualizované)."""
        now = utcnow()
        added = updated = 0
        async with self.engine.begin() as conn:
            existing = {r[0] for r in await conn.execute(select(s.contacts.c.public_key))}
            for c in contacts:
                key = c["public_key"]
                if key in existing:
                    if c.get("favorite"):
                        await conn.execute(update(s.contacts).where(s.contacts.c.public_key == key)
                                           .values(favorite=True, updated_at=now))
                        updated += 1
                    continue
                await conn.execute(s.contacts.insert().values(
                    public_key=key, name=c.get("name"), type=c.get("type"), flags=c.get("flags") or 0,
                    out_path=None, out_path_len=-1, lat=c.get("lat"), lon=c.get("lon"),
                    favorite=bool(c.get("favorite")), on_device=False, first_seen_at=now, updated_at=now,
                ))
                added += 1
        return added, updated


    # --------------------------------------------- přihlášení k uzlům (room/repeater)

    async def get_login(self, key: str) -> Optional[dict]:
        r = s.remote_logins
        async with self.engine.connect() as conn:
            row = (await conn.execute(select(r).where(r.c.contact_key == key))).mappings().first()
            return dict(row) if row else None

    async def list_logins(self) -> list[dict]:
        async with self.engine.connect() as conn:
            return [dict(r) for r in (await conn.execute(select(s.remote_logins))).mappings()]

    async def save_login(self, key: str, **values) -> None:
        async with self.engine.begin() as conn:
            await _upsert(conn, s.remote_logins, [{"contact_key": key, "updated_at": utcnow(), **values}],
                          ["contact_key"])

    # ------------------------------------------------------- sledované uzly

    async def list_tracked(self) -> list[dict]:
        t, c = s.tracked_nodes, s.contacts
        stmt = (select(t, c.c.name, c.c.type).select_from(t.outerjoin(c, c.c.public_key == t.c.contact_key))
                .order_by(c.c.name))
        async with self.engine.connect() as conn:
            return [dict(r) for r in (await conn.execute(stmt)).mappings()]

    async def get_tracked(self, key: str) -> Optional[dict]:
        t, c = s.tracked_nodes, s.contacts
        stmt = (select(t, c.c.name, c.c.type).select_from(t.outerjoin(c, c.c.public_key == t.c.contact_key))
                .where(t.c.contact_key == key))
        async with self.engine.connect() as conn:
            row = (await conn.execute(stmt)).mappings().first()
            return dict(row) if row else None

    async def add_tracked(self, key: str) -> None:
        now = utcnow()
        async with self.engine.begin() as conn:
            exists = (await conn.execute(select(s.tracked_nodes.c.contact_key)
                                         .where(s.tracked_nodes.c.contact_key == key))).first()
            if exists:
                await conn.execute(update(s.tracked_nodes).where(s.tracked_nodes.c.contact_key == key)
                                   .values(next_poll_at=now, updated_at=now))
            else:
                await conn.execute(s.tracked_nodes.insert().values(
                    contact_key=key, added_at=now, next_poll_at=now, failures=0, updated_at=now))

    async def remove_tracked(self, key: str) -> bool:
        async with self.engine.begin() as conn:
            n = (await conn.execute(delete(s.tracked_nodes).where(s.tracked_nodes.c.contact_key == key))).rowcount
            await conn.execute(delete(s.node_samples).where(s.node_samples.c.contact_key == key))
            return (n or 0) > 0

    async def due_tracked(self, now: datetime) -> list[str]:
        t = s.tracked_nodes
        async with self.engine.connect() as conn:
            rows = await conn.execute(select(t.c.contact_key).where(t.c.next_poll_at <= now).order_by(t.c.next_poll_at))
            return [r[0] for r in rows]

    async def update_tracked(self, key: str, **values) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(update(s.tracked_nodes).where(s.tracked_nodes.c.contact_key == key)
                               .values(updated_at=utcnow(), **values))

    async def add_node_sample(self, key: str, at: datetime, data: dict) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(s.node_samples.insert().values(contact_key=key, at=at, data=data))

    async def node_samples(self, key: str, since: datetime) -> list[dict]:
        n = s.node_samples
        async with self.engine.connect() as conn:
            rows = await conn.execute(select(n.c.at, n.c.data).where(n.c.contact_key == key, n.c.at >= since).order_by(n.c.at))
            return [dict(r) for r in rows.mappings()]

    async def prune_node_samples(self, before: datetime) -> int:
        async with self.engine.begin() as conn:
            return (await conn.execute(delete(s.node_samples).where(s.node_samples.c.at < before))).rowcount or 0

    async def mark_all_logged_out(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(update(s.remote_logins).where(s.remote_logins.c.logged_in.is_(True))
                               .values(logged_in=False, updated_at=utcnow()))
