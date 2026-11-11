#!/usr/bin/env python3
"""Jednoduchý falešný MeshCore companion přes TCP – pro vývoj bez rádia.

Odpovídá na příkazy, které server používá (APP_START, info o zařízení,
nastavení času, kontakty, kanály, vyzvednutí zpráv, baterie) a v zadaném
intervalu střídavě „přijme“ zprávu do kanálu Public, DM od Alice a advert.

Použití:
    python tools/fake_companion.py --port 5000 --interval 20
    MCS_COMPANION__TCP__HOST=127.0.0.1 meshcore-server run

Neúplná implementace protokolu; plnohodnotný simulátor přijde později.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import os
import struct
import time

from Crypto.Cipher import AES  # pycryptodome, instaluje se s knihovnou meshcore

STATE = {"name": "FakeCompanion", "lat": 50_087_000, "lon": 14_421_000, "tx": 22,
         "freq": 869_525, "bw": 250_000, "sf": 11, "cr": 5, "scope": None}
PUBKEY = bytes.fromhex("ab" * 32)
CHANNELS = {0: "Public", 1: "test"}
CONTACTS = [
    (bytes.fromhex("11" * 32), 1, "Alice"),
    (bytes.fromhex("22" * 32), 2, "Repeater-Plzen"),
    (bytes.fromhex("55" * 32), 3, "Room-Plzen"),
]
ROOM_KEY = bytes.fromhex("55" * 32)
ROOM_PASSWORD = "heslo"   # admin; prázdné heslo = host
MAX_CONTACTS = 50  # firmware posílá max_contacts / 2 (viz device_info)
MAX_CHANNELS = 8


def frame(payload: bytes) -> bytes:
    return b"\x3e" + len(payload).to_bytes(2, "little") + payload


def self_info() -> bytes:
    st = STATE
    return (
        bytes([5, 1, st["tx"], 22]) + PUBKEY
        + struct.pack("<ii", st["lat"], st["lon"])
        + bytes([0, 0, 0, 0])
        + struct.pack("<II", st["freq"], st["bw"]) + bytes([st["sf"], st["cr"]])
        + st["name"].encode()
    )


def device_info() -> bytes:
    def fixed(text: str, size: int) -> bytes:
        return text.encode()[:size].ljust(size, b"\0")

    return (
        bytes([13, 9, MAX_CONTACTS // 2, MAX_CHANNELS]) + (123456).to_bytes(4, "little")
        + fixed("11-Nov-2026", 12) + fixed("Fake Companion", 40) + fixed("v1.10.0-fake", 20)
        + bytes([0])
    )


def channel_secret(idx: int) -> bytes:
    return hashlib.sha256(f"fake-{idx}".encode()).digest()[:16]


def channel_info(idx: int) -> bytes:
    name = CHANNELS.get(idx, "")
    return bytes([18, idx]) + name.encode().ljust(32, b"\0") + channel_secret(idx)


def heard_repeat(idx: int, ts: bytes, full_text: bytes, path: bytes) -> bytes:
    """LOG_RX_DATA s naší kanálovou zprávou, jak ji zopakoval repeater (2B hashe hopů)."""
    secret = channel_secret(idx)
    plain = ts + bytes([0]) + full_text
    plain += b"\0" * (-len(plain) % 16)
    cipher = AES.new(secret, AES.MODE_ECB).encrypt(plain)
    mac = hmac.new(secret, cipher, hashlib.sha256).digest()[:2]
    chan_hash = hashlib.sha256(secret).digest()[:1]
    header = 0x01 | (0x05 << 2)                     # flood, GRP_TXT
    path_byte = (1 << 6) | (len(path) // 2)          # 2bajtové hashe
    packet = bytes([header, path_byte]) + path + chan_hash + mac + cipher
    return bytes([0x88, 40, (-80) & 0xFF]) + packet


def contact_frame(pubkey: bytes, ctype: int, name: str) -> bytes:
    now = int(time.time())
    return (
        bytes([3]) + pubkey + bytes([ctype, 0, 1]) + bytes([0xAB]).ljust(64, b"\0")
        + name.encode().ljust(32, b"\0")
        + now.to_bytes(4, "little") + struct.pack("<ii", 49_747_000, 13_377_000)
        + now.to_bytes(4, "little")
    )


def dm_msg(pubkey: bytes, text: str) -> bytes:
    # CONTACT_MSG_RECV_V3: SNR, 2 B rezerva, prefix klíče, délka trasy, typ, čas, text
    return (
        bytes([16, 38, 0, 0]) + pubkey[:6] + bytes([1, 0])
        + int(time.time()).to_bytes(4, "little") + text.encode()
    )


def room_post(author: bytes, text: str) -> bytes:
    # CONTACT_MSG_RECV_V3 s txt_type 2: od room serveru, autor v „signature“ (4 B)
    return (
        bytes([16, 40, 0, 0]) + ROOM_KEY[:6] + bytes([2, 2])
        + int(time.time()).to_bytes(4, "little") + author[:4] + text.encode()
    )


def channel_msg(text: str) -> bytes:
    return bytes([8, 0, 2, 0]) + int(time.time()).to_bytes(4, "little") + text.encode()


class FakeCompanion:
    def __init__(self, interval: float, ack_mode: str = "always") -> None:
        self.interval = interval
        self.counter = 0
        self.ack_mode = ack_mode   # always | never | retry (potvrdí až druhý pokus)
        self.scope = None
        self.writer = None

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        print(f"klient připojen: {peer}")
        queue: list[bytes] = []
        self.writer = writer
        pusher = asyncio.create_task(self._push_messages(writer, queue))
        try:
            while True:
                header = await reader.readexactly(3)
                if header[0] != 0x3C:
                    continue
                data = await reader.readexactly(int.from_bytes(header[1:], "little"))
                for reply in self._reply(data, queue):
                    writer.write(frame(reply))
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            pusher.cancel()
            writer.close()
            print(f"klient odpojen: {peer}")

    def _reply(self, data: bytes, queue: list[bytes]) -> list[bytes]:
        cmd = data[0]
        if cmd == 0x01:   # APP_START
            return [self_info()]
        if cmd == 0x16:   # DEVICE_QUERY
            return [device_info()]
        if cmd == 0x06:   # SET_TIME
            print(f"čas nastaven na {int.from_bytes(data[1:5], 'little')}")
            return [bytes([0])]
        if cmd == 0x04:   # GET_CONTACTS
            since = int.from_bytes(data[1:5], "little") if len(data) >= 5 else 0
            items = [contact_frame(*c) for c in CONTACTS] if since == 0 else [contact_frame(*CONTACTS[0])]
            return (
                [bytes([2]) + len(items).to_bytes(4, "little")] + items
                + [bytes([4]) + int(time.time()).to_bytes(4, "little")]
            )
        if cmd == 0x1F:   # GET_CHANNEL
            idx = data[1]
            return [channel_info(idx)] if idx < MAX_CHANNELS else [bytes([1, 2])]
        if cmd == 0x0A:   # SYNC_NEXT_MESSAGE
            if getattr(self, "_room_queue", None):
                return [self._room_queue.pop(0)]
            return [queue.pop(0)] if queue else [bytes([10])]
        if cmd == 0x02 and data[1] == 1:   # CLI příkaz na repeater/room (txt_type CLI_DATA)
            prefix, command = data[7:13], data[13:].decode(errors="replace")
            print(f"CLI na {prefix.hex()}: {command!r}")
            reply = {"ver": "v1.18.0 (fake)", "get radio": "> 869.525,250.0,11,5"}.get(command, f"? {command}")
            # odpověď jako CONTACT_MSG_RECV_V3 s txt_type 1
            msg = (bytes([16, 40, 0, 0]) + prefix + bytes([0, 1]) + int(time.time()).to_bytes(4, "little")
                   + reply.encode())
            self._room_queue = getattr(self, "_room_queue", []) + [msg]
            asyncio.get_running_loop().call_later(0.6, self._push, bytes([0x83]))
            return [bytes([6, 1]) + os.urandom(4) + (3000).to_bytes(4, "little")]
        if cmd == 0x02:   # SEND_TXT_MSG: 0x02 0x00 pokus čas(4) prefix(6) text
            attempt = data[2]
            text = data[13:].decode(errors="replace")
            ack = os.urandom(4)
            print(f"DM pro {data[7:13].hex()} (pokus {attempt}): {text!r}")
            if self.ack_mode == "always" or (self.ack_mode == "retry" and attempt >= 1):
                asyncio.get_running_loop().call_later(1.0, self._push_ack, ack)
            return [bytes([6, 0]) + ack + (2000).to_bytes(4, "little")]
        if cmd == 0x03:   # SEND_CHANNEL_TXT_MSG: 0x03 0x00 kanál čas(4) text
            idx, ts, text = data[2], data[3:7], data[7:]
            print(f"kanál {idx} (region {self.scope or 'výchozí'}): {text.decode(errors='replace')!r}")
            # Zprávu „zopakují“ dva repeatery; companion je zaslechne v RX logu.
            full = STATE["name"].encode() + b": " + text
            loop = asyncio.get_running_loop()
            for delay, path in ((0.5, bytes.fromhex("aaaa2222")), (1.2, bytes.fromhex("bbbbcccc")),
                                (1.5, bytes.fromhex("aaaa2222"))):
                loop.call_later(delay, self._push, heard_repeat(idx, ts, full, path))
            return [bytes([0])]
        if cmd == 0x1A:   # SEND_LOGIN: klíč(32) heslo
            key, pwd = data[1:33], data[33:].decode(errors="replace")
            print(f"přihlášení k {key.hex()[:12]} heslem {pwd!r}")
            if key == ROOM_KEY:
                loop = asyncio.get_running_loop()
                if pwd in (ROOM_PASSWORD, ""):
                    perms = 1 if pwd == ROOM_PASSWORD else 0
                    ok = bytes([0x85, perms]) + key[:6] + int(time.time()).to_bytes(4, "little") + bytes([perms, 1])
                    loop.call_later(1.0, self._push, ok)
                    # po přihlášení room pošle příspěvky ostatních
                    self._room_queue = [room_post(CONTACTS[0][0], "Ahoj všem v roomu"),
                                        room_post(bytes.fromhex("99" * 4), "Zpráva od neznámého autora")]
                    loop.call_later(1.5, self._push, bytes([0x83]))
                else:
                    loop.call_later(1.0, self._push, bytes([0x86, 0]) + key[:6])
            return [bytes([6, 1]) + os.urandom(4) + (3000).to_bytes(4, "little")]
        if cmd == 0x32:   # BINARY_REQ: klíč(32) typ data
            key, rtype = data[1:33], data[33]
            tag = os.urandom(4)
            payload = None
            if rtype == 1:    # STATUS: 52 B polí (baterie, fronta, šum, RSSI, pakety, airtime, uptime…)
                payload = struct.pack("<HHhhIIIIIIIIHhHHI", 4120, 0, -112, -85, 1500, 900, 3600, 86400,
                                      400, 500, 700, 800, 0, 40, 3, 7, 1800)
            elif rtype == 3:  # TELEMETRY: Cayenne LPP – kanál 1 napětí 4.12 V, kanál 2 teplota 21.5 °C
                payload = bytes([1, 116]) + (412).to_bytes(2, "big") + bytes([2, 103]) + (215).to_bytes(2, "big")
            elif rtype == 6:  # NEIGHBOURS: celkem, vráceno, [klíč(4) stáří(4) snr(1)]
                payload = (2).to_bytes(2, "little") + (2).to_bytes(2, "little")
                payload += bytes.fromhex("22222222") + (120).to_bytes(4, "little") + bytes([40])
                payload += bytes.fromhex("77777777") + (3600).to_bytes(4, "little") + bytes([(-20) & 0xFF])
            elif rtype == 5:  # ACL: [klíč(6) oprávnění(1)]
                payload = CONTACTS[0][0][:6] + bytes([3]) + PUBKEY[:6] + bytes([1])
            if key in (ROOM_KEY, CONTACTS[1][0]) and payload is not None:
                asyncio.get_running_loop().call_later(
                    0.8, self._push, bytes([0x8C, 0]) + tag + payload)
            print(f"binární požadavek {rtype} na {key.hex()[:12]}")
            return [bytes([6, 1]) + tag + (3000).to_bytes(4, "little")]
        if cmd == 0x1D:   # LOGOUT
            print("odhlášení")
            return [bytes([0])]
        if cmd == 54:     # SET_FLOOD_SCOPE
            key = data[2:18]
            self.scope = None if key == bytes(16) else key.hex()[:8]
            return [bytes([0])]
        if cmd == 0x0D:   # RESET_PATH
            print("reset trasy")
            return [bytes([0])]
        if cmd == 64:     # GET_DEFAULT_FLOOD_SCOPE
            if not STATE["scope"]:
                return [bytes([28])]
            return [bytes([28]) + STATE["scope"].encode().ljust(31, b"\0") + bytes(16)]
        if cmd == 63:     # SET_DEFAULT_FLOOD_SCOPE: jméno(31) klíč(16)
            STATE["scope"] = data[1:32].split(b"\0")[0].decode() or None
            print(f"výchozí region: {STATE['scope']}")
            return [bytes([0])]
        if cmd == 0x07:   # SEND_SELF_ADVERT
            print(f"advert ({'flood' if len(data) > 1 and data[1] else 'zero-hop'})")
            return [bytes([0])]
        if cmd == 0x08:   # SET_ADVERT_NAME
            STATE["name"] = data[1:].decode()
            return [bytes([0])]
        if cmd == 0x0E:   # SET_ADVERT_LATLON
            STATE["lat"], STATE["lon"] = struct.unpack("<ii", data[1:9])
            return [bytes([0])]
        if cmd == 0x0C:   # SET_RADIO_TX_POWER
            STATE["tx"] = int.from_bytes(data[1:5], "little")
            return [bytes([0])]
        if cmd == 0x0B:   # SET_RADIO_PARAMS
            STATE["freq"], STATE["bw"] = struct.unpack("<II", data[1:9])
            STATE["sf"], STATE["cr"] = data[9], data[10]
            print(f"rádio: {STATE['freq']/1000} MHz, BW {STATE['bw']/1000}, SF{STATE['sf']}, CR{STATE['cr']}")
            return [bytes([0])]
        if cmd == 0x13:   # REBOOT
            print("restart!")
            raise ConnectionResetError
        if cmd == 9:      # ADD_UPDATE_CONTACT: klíč(32) typ flags délka trasy(1) trasa(64) jméno(32)…
            key, ctype = data[1:33], data[33]
            name = data[100:132].split(b"\0")[0].decode(errors="replace")
            for i, c in enumerate(CONTACTS):
                if c[0] == key:
                    CONTACTS[i] = (key, ctype, name)
                    break
            else:
                if len(CONTACTS) >= MAX_CONTACTS:
                    return [bytes([1, 3])]   # ERR_CODE_TABLE_FULL
                CONTACTS.append((key, ctype, name))
            print(f"kontakt uložen: {name} ({key.hex()[:12]})")
            return [bytes([0])]
        if cmd == 15:     # REMOVE_CONTACT
            key = data[1:33]
            before = len(CONTACTS)
            CONTACTS[:] = [c for c in CONTACTS if c[0] != key]
            print(f"kontakt odebrán: {key.hex()[:12]}")
            return [bytes([0])] if len(CONTACTS) < before else [bytes([1, 2])]
        if cmd == 30:     # GET_CONTACT_BY_KEY
            key = data[1:33]
            for c in CONTACTS:
                if c[0] == key:
                    return [contact_frame(*c)]
            return [bytes([1, 2])]
        if cmd == 32:     # SET_CHANNEL: idx jméno(32) klíč(16)
            idx = data[1]
            name = data[2:34].split(b"\0")[0].decode(errors="replace")
            if name:
                CHANNELS[idx] = name
            else:
                CHANNELS.pop(idx, None)
            print(f"kanál {idx} nastaven: {name or '(smazán)'}")
            return [bytes([0])]
        if cmd == 38:     # SET_OTHER_PARAMS
            print(f"manual_add_contacts = {bool(data[1])}")
            return [bytes([0])]
        if cmd == 0x14:   # GET_BATTERY
            return [bytes([12]) + (4100).to_bytes(2, "little")]
        print(f"neznámý příkaz 0x{cmd:02x}")
        return [bytes([1, 1])]

    def _push(self, payload: bytes) -> None:
        if self.writer is not None and not self.writer.is_closing():
            self.writer.write(frame(payload))

    def _push_ack(self, ack: bytes) -> None:
        if self.writer is not None and not self.writer.is_closing():
            self.writer.write(frame(bytes([0x82]) + ack + (850).to_bytes(4, "little")))

    async def _push_messages(self, writer: asyncio.StreamWriter, queue: list[bytes]) -> None:
        while self.interval > 0:
            await asyncio.sleep(self.interval)
            self.counter += 1
            if self.counter % 3 == 0:
                writer.write(frame(bytes([0x80]) + CONTACTS[0][0]))  # ADVERTISEMENT
            elif self.counter % 3 == 1:
                queue.append(channel_msg(f"Tester: testovací zpráva č. {self.counter}"))
                writer.write(frame(bytes([0x83])))  # MESSAGES_WAITING
            else:
                queue.append(dm_msg(CONTACTS[0][0], f"Ahoj, DM č. {self.counter}"))
                writer.write(frame(bytes([0x83])))
            await writer.drain()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--interval", type=float, default=20, help="s mezi zprávami (0 = žádné)")
    parser.add_argument("--max-contacts", type=int, default=50, help="kapacita kontaktů (sudé číslo)")
    parser.add_argument("--ack", choices=["always", "never", "retry"], default="always",
                        help="potvrzování DM: vždy, nikdy, až opakovaný pokus")
    args = parser.parse_args()

    global MAX_CONTACTS
    MAX_CONTACTS = args.max_contacts
    fake = FakeCompanion(args.interval, args.ack)
    server = await asyncio.start_server(fake.handle, args.host, args.port)
    print(f"Falešný companion poslouchá na {args.host}:{args.port}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
