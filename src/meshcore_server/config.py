"""Načtení a validace konfigurace.

Pořadí zdrojů (pozdější přepisuje dřívější):
  1. výchozí hodnoty v modelech níže
  2. YAML soubor (--config, proměnná MCS_CONFIG, ./config.yaml, /etc/meshcore-server/config.yaml)
  3. proměnné prostředí s prefixem MCS_, vnoření se odděluje dvěma podtržítky:
       MCS_COMPANION__TCP__HOST=192.168.1.160
       MCS_COMPANION__TCP__PORT=5000
       MCS_LOGGING__LEVEL=DEBUG
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal, Optional

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

ENV_PREFIX = "MCS_"
CONFIG_ENV = "MCS_CONFIG"
DEFAULT_CONFIG_PATHS = (
    Path("config.yaml"),
    Path("/etc/meshcore-server/config.yaml"),
)


class ConfigError(Exception):
    """Chyba v konfiguraci, kterou má smysl ukázat uživateli."""


class _Strict(BaseModel):
    model_config = {"extra": "forbid"}


class TcpConfig(_Strict):
    host: Optional[str] = None
    port: int = Field(5000, ge=1, le=65535)


class SerialConfig(_Strict):
    port: Optional[str] = None
    baudrate: int = 115200


class BleConfig(_Strict):
    address: Optional[str] = None
    pin: Optional[str] = None


class ReconnectConfig(_Strict):
    initial_delay: float = Field(2.0, gt=0)
    max_delay: float = Field(60.0, gt=0)


class CompanionConfig(_Strict):
    transport: Literal["tcp", "serial", "ble"] = "tcp"
    tcp: TcpConfig = TcpConfig()
    serial: SerialConfig = SerialConfig()
    ble: BleConfig = BleConfig()
    command_timeout: float = Field(10.0, gt=0)
    keepalive_interval: float = Field(30.0, ge=0)  # 0 = vypnuto
    keepalive_failures: int = Field(2, ge=1)
    set_time_on_connect: bool = True
    decrypt_channel_logs: bool = True   # doplnit u kanálových zpráv trasu, SNR a RSSI
    allow_device_change: bool = False   # povolit připojení jiného companionu, než je v DB
    auto_add_contacts_on_send: bool = True  # DM kontaktu mimo companion ho tam nejdřív přidá
    dm_max_attempts: int = Field(3, ge=1, le=10)        # pokusy o doručení DM (poslední floodem)
    message_expiry_minutes: int = Field(10, ge=1)       # jak dlouho může zpráva čekat na odeslání
    # MAX_TEXT_LEN firmwaru (bajty UTF-8). U kanálu se do něj počítá i „jméno: “.
    max_text_bytes: int = Field(160, ge=10, le=160)
    reconnect: ReconnectConfig = ReconnectConfig()

    @model_validator(mode="after")
    def _check_transport(self) -> "CompanionConfig":
        if self.transport == "tcp" and not self.tcp.host:
            raise ValueError("companion.tcp.host musí být vyplněn (IP adresa companionu)")
        if self.transport == "serial" and not self.serial.port:
            raise ValueError("companion.serial.port musí být vyplněn (např. /dev/ttyUSB0)")
        return self

    def describe(self) -> str:
        """Lidsky čitelný popis spojení, např. pro log."""
        if self.transport == "tcp":
            return f"TCP {self.tcp.host}:{self.tcp.port}"
        if self.transport == "serial":
            return f"serial {self.serial.port} @ {self.serial.baudrate}"
        return f"BLE {self.ble.address or '(automatické hledání)'}"


class ServerConfig(_Strict):
    host: str = "0.0.0.0"
    port: int = Field(8080, ge=1, le=65535)
    docs_enabled: bool = True           # dokumentace API na /docs
    cors_origins: list[str] = []        # povolené originy pro volání z prohlížeče
    trusted_proxies: list[str] = ["127.0.0.1"]  # IP reverse proxy, kterým věřit X-Forwarded-For


class MysqlConfig(_Strict):
    host: str = "localhost"
    port: int = 3306
    database: str = "meshcore_server"
    user: str = "meshcore"
    password: str = ""


class RetentionConfig(_Strict):
    """Automatické pročišťování; 0 = nemazat (výchozí – historie bez limitu)."""

    messages_days: int = Field(0, ge=0)
    adverts_days: int = Field(0, ge=0)
    commands_days: int = Field(0, ge=0)


class DatabaseConfig(_Strict):
    driver: Literal["sqlite", "mysql"] = "sqlite"
    sqlite_path: str = "data/meshcore.db"
    mysql: MysqlConfig = MysqlConfig()
    auto_migrate: bool = True  # při startu převést schéma na nejnovější verzi
    events_retention_days: int = Field(7, ge=1)  # jak dlouho držet události pro doplnění zmeškaných
    retention: RetentionConfig = RetentionConfig()


class LoggingConfig(_Strict):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_events: bool = True        # vypisovat události z companionu
    log_noisy_events: bool = False  # i časté ladicí události (rx_log_data, log_data…)


class Config(_Strict):
    companion: CompanionConfig
    server: ServerConfig = ServerConfig()
    database: DatabaseConfig = DatabaseConfig()
    logging: LoggingConfig = LoggingConfig()

    @model_validator(mode="before")
    @classmethod
    def _ensure_companion(cls, data: Any) -> Any:
        # Sekce companion se validuje vždy, i když v souboru chybí,
        # aby se uživatel dozvěděl, že chybí IP adresa.
        if isinstance(data, dict) and data.get("companion") is None:
            data = {**data, "companion": {}}
        return data


def _find_config_file(explicit: Optional[str]) -> Optional[Path]:
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise ConfigError(f"Konfigurační soubor nenalezen: {path}")
        return path
    env_path = os.environ.get(CONFIG_ENV)
    if env_path:
        path = Path(env_path)
        # V Dockeru může být MCS_CONFIG nastavena, i když soubor není připojen.
        return path if path.is_file() else None
    for path in DEFAULT_CONFIG_PATHS:
        if path.is_file():
            return path
    return None


_STRING_KEYS = {"host", "address", "pin", "password", "user", "database", "sqlite_path"}


def _set_nested(data: dict, keys: list[str], value: Any) -> None:
    node = data
    for key in keys[:-1]:
        existing = node.get(key)
        if not isinstance(existing, dict):
            existing = {}
            node[key] = existing
        node = existing
    node[keys[-1]] = value


def _env_overrides(environ: dict[str, str]) -> dict:
    result: dict = {}
    for name, raw in environ.items():
        if not name.startswith(ENV_PREFIX) or name == CONFIG_ENV:
            continue
        keys = [k.lower() for k in name[len(ENV_PREFIX):].split("__") if k]
        if not keys:
            continue
        # YAML parser převede "5000" na int, "true" na bool apod.
        try:
            value = yaml.safe_load(raw) if raw != "" else None
        except yaml.YAMLError:
            value = raw
        # Hodnoty, které jsou vždy řetězec, nepřevádíme (IP adresa, PIN, heslo…).
        if keys[-1] in _STRING_KEYS:
            value = raw
        _set_nested(result, keys, value)
    return result


def _deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: Optional[str] = None, environ: Optional[dict[str, str]] = None) -> tuple[Config, Optional[Path]]:
    """Vrátí (konfigurace, cesta k použitému souboru nebo None)."""
    environ = dict(os.environ) if environ is None else environ
    config_path = _find_config_file(path)

    data: dict = {}
    if config_path:
        try:
            loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise ConfigError(f"Chyba v YAML souboru {config_path}: {exc}") from exc
        if not isinstance(loaded, dict):
            raise ConfigError(f"{config_path}: kořen konfigurace musí být mapa (klíč: hodnota)")
        data = loaded

    data = _deep_merge(data, _env_overrides(environ))

    try:
        return Config.model_validate(data), config_path
    except ValidationError as exc:
        lines = []
        for err in exc.errors():
            loc = ".".join(str(p) for p in err["loc"]) or "(kořen)"
            msg = err["msg"].removeprefix("Value error, ")
            lines.append(f"  {loc}: {msg}")
        raise ConfigError("Neplatná konfigurace:\n" + "\n".join(lines)) from exc
