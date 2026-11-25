# meshcore-server

Samostatný server pro [MeshCore](https://github.com/meshcore-dev/MeshCore) companion. Drží jediné spojení s rádiem (TCP/WiFi, USB serial nebo BLE), ukládá přijatá data do databáze a nabízí API pro klienty, např. [meshcore-klient](https://github.com/pmalecek/meshcore-klient). Nepotřebuje Home Assistant.

## Co umí

- **Spojení s companionem** přes TCP/WiFi, USB serial nebo BLE; hlídá ho a po výpadku se sám znovu připojí.
- **Databáze** (SQLite nebo MySQL/MariaDB): companion a jeho nastavení, kontakty, kanály, zprávy z kanálů, DM i room serverů (s trasou a jmény repeaterů), adverty.
- **Odesílání zpráv** přes frontu, která přežije restart i výpadek companionu: kanály a kontakty s regionem (flood scope), DM s potvrzením a opakováním, příspěvky do room serverů; u kanálových zpráv počítá, kolikrát je zaslechl zopakované repeatery.
- **Správa companionu:** kontakty (oblíbené, automatické uvolňování místa, import vizitky), kanály, jméno, poloha, výkon, parametry rádia, advert, restart.
- **Room servery a repeatery:** přihlášení (i automatické po reconnectu), stav, telemetrie, sousedé, ACL a CLI příkazy.
- **Cesty:** zjištění cesty ke kontaktu a trace se SNR na každém skoku.
- **Sledování uzlů:** pravidelný dotaz na stav repeaterů a room serverů a na telemetrii klientů, online/offline a historie hodnot pro grafy.
- **API:** REST a WebSocket se živými událostmi a doplněním zmeškaných, tokeny s oprávněním read/write/admin, nepřečtené zprávy pro jednotlivé uživatele klienta, podrobný stav serveru.
- **Provoz:** Docker i pip/systemd, migrace schématu, pročišťování historie, zálohy, export a import kontaktů a kanálů.

## Požadavky

- Python 3.11 nebo novější (pro instalaci přes pip), nebo Docker
- MeshCore companion s firmwarem pro TCP (WiFi), USB serial nebo BLE

## Konfigurace

Zkopírujte `config.example.yaml` jako `config.yaml` a vyplňte alespoň IP adresu companionu:

```yaml
companion:
  transport: tcp
  tcp:
    host: 192.168.1.160
    port: 5000
```

Server hledá konfiguraci v tomto pořadí: `--config`, proměnná `MCS_CONFIG`, `./config.yaml`, `/etc/meshcore-server/config.yaml`. Každou hodnotu lze přepsat proměnnou prostředí s prefixem `MCS_` (vnoření dvěma podtržítky):

```bash
MCS_COMPANION__TCP__HOST=192.168.1.161 meshcore-server run
```

Kontrola konfigurace bez připojení:

```bash
meshcore-server check-config
```

## Databáze

Výchozí je SQLite (soubor `data/meshcore.db`), alternativou MySQL/MariaDB. Schéma se při startu automaticky převede na nejnovější verzi (`database.auto_migrate`).

```yaml
database:
  driver: mysql
  mysql:
    host: localhost
    port: 3306
    database: meshcore_server
    user: meshcore
    password: tajne-heslo
```

Pro MySQL je potřeba doinstalovat ovladač: `pip install ".[mysql]"` (Docker image ho obsahuje). Databázi a uživatele vytvořte předem:

```sql
CREATE DATABASE meshcore_server CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'meshcore'@'localhost' IDENTIFIED BY 'tajne-heslo';
GRANT ALL ON meshcore_server.* TO 'meshcore'@'localhost';
```

Příkazy pro správu:

```bash
meshcore-server db migrate   # převede schéma na nejnovější verzi
meshcore-server db info      # typ DB, verze schématu, počty záznamů
meshcore-server db purge     # pročištění historie (viz níže)
meshcore-server db backup    # záloha SQLite
```

### Pročišťování, zálohy, export

Historie se ve výchozím stavu nemaže. Automatické pročišťování (jednou za hodinu) zapnete v konfiguraci:

```yaml
database:
  retention:
    messages_days: 365   # 0 = nemazat
    adverts_days: 30
    commands_days: 90
```

Ruční pročištění (s náhledem):

```bash
meshcore-server db purge --messages-days 365 --dry-run
meshcore-server db purge --messages-days 365
```

**Záloha SQLite** (lze i za běhu serveru, používá online backup API SQLite – na rozdíl od kopírování souboru je kopie vždy konzistentní):

```bash
meshcore-server db backup                    # → backups/meshcore-<datum>.db
meshcore-server db backup /mnt/nas/meshcore.db
```

U MySQL použijte `mysqldump --single-transaction` (příkaz ho vypíše).

**Export kontaktů a kanálů** do JSON – příkazem `meshcore-server export soubor.json` nebo `GET /api/v1/export` (admin). Soubor obsahuje klíče kanálů, proto se ukládá s právy jen pro vlastníka; nesdílejte ho. Import přes `POST /api/v1/import` (admin):

- kontakty se přidají do databáze (do companionu až při potřebě), u existujících se převezme jen příznak oblíbený
- kanály podle `?channels=`: `regions` (výchozí) převezme jen regiony kanálů, které v companionu už jsou; `device` kanály zapíše do companionu (přepíše dané sloty); `skip` je ignoruje

Server obsluhuje vždy jeden companion. Pokud se připojí jiný (s jiným public key), než je uložený v databázi, server nic neuloží a hlásí chybu. Záměrnou výměnu povolíte nastavením `companion.allow_device_change: true`.

## API

Všechny endpointy kromě `/health` vyžadují API token. Vytvoříte ho příkazem (token se zobrazí jen jednou):

```bash
meshcore-server token create meshcore-klient --scope read   # read < write < admin
meshcore-server token list
meshcore-server token revoke meshcore-klient
```

Token se posílá v hlavičce `Authorization: Bearer <token>`. Interaktivní dokumentace je na `http://<server>:8080/docs` (vypnete `server.docs_enabled: false`).

| Metoda | Cesta | Popis |
| --- | --- | --- |
| GET | `/health` | Stav pro healthcheck (bez tokenu) |
| GET | `/api/v1/status` | Verze, uptime, stav spojení s companionem, baterie companionu, poslední `seq` |
| GET | `/api/v1/device` | Companion a jeho nastavení |
| GET | `/api/v1/contacts` | Kontakty; filtry `type`, `on_device`, `q` |
| GET | `/api/v1/contacts/{key}` | Kontakt podle klíče nebo jeho prefixu |
| GET | `/api/v1/channels` | Kanály |
| GET | `/api/v1/messages` | Zprávy od nejnovějších; `kind`, `peer`, `before`, `after`, `limit` |
| GET | `/api/v1/adverts` | Přijaté adverty; `key`, `before`, `limit` |
| POST | `/api/v1/ws-ticket` | Jednorázový lístek pro WebSocket z prohlížeče (platí 60 s) |
| POST | `/api/v1/messages` | Odeslání zprávy (oprávnění `write`), viz níže |
| PATCH | `/api/v1/channels/{idx}` | Jen region (flood scope) kanálu (`write`) |
| PUT | `/api/v1/channels/{idx}` | Nastavení kanálu: `name`, `secret`, `region` (`write`) |
| DELETE | `/api/v1/channels/{idx}` | Smazání kanálu z companionu (`write`) |
| PATCH | `/api/v1/contacts/{key}` | `favorite` – oblíbený se z companionu nikdy neodebere; `region` – region pro odesílání kontaktu floodem (`write`) |
| POST | `/api/v1/contacts/{key}/device` | Přidání kontaktu z DB do companionu (`write`) |
| DELETE | `/api/v1/contacts/{key}/device` | Odebrání z companionu, v DB zůstane (`write`) |
| DELETE | `/api/v1/contacts/{key}` | Smazání z companionu i DB, zprávy zůstanou (`admin`) |
| POST | `/api/v1/contacts/{key}/reset-path` | Zapomenutí přímé trasy ke kontaktu (`write`) |
| POST | `/api/v1/device/advert` | Odeslání advertu, `{"flood": true}` přes repeatery (`write`) |
| PATCH | `/api/v1/device` | Jméno, poloha, výkon, parametry rádia, výchozí region (`admin`) |
| POST | `/api/v1/sync` | Znovu načíst vše z companionu (`write`) |
| POST | `/api/v1/device/reboot` | Restart companionu; server se připojí znovu sám (`admin`) |
| GET | `/api/v1/unread` | Nepřečtené zprávy podle konverzací; `reader` = uživatel klienta |
| POST | `/api/v1/read` | Označení konverzace jako přečtené po danou zprávu |
| GET | `/api/v1/server` | Podrobný stav: databáze, vytížení, companion, klienti (`admin`) |
| POST | `/api/v1/contacts/{key}/login` | Přihlášení k room serveru nebo repeateru (`write`) |
| POST | `/api/v1/contacts/{key}/logout` | Odhlášení (`write`) |
| GET | `/api/v1/contacts/{key}/login` | Stav přihlášení |
| POST | `/api/v1/contacts/{key}/status` | Stav repeateru/room serveru (`write`) |
| POST | `/api/v1/contacts/{key}/telemetry` | Telemetrie uzlu (`write`) |
| POST | `/api/v1/contacts/{key}/path-discovery` | Zjistit cestu (flood dotaz; companion si cestu uloží) (`write`) |
| POST | `/api/v1/contacts/{key}/trace` | Trace tam a zpět: SNR na skocích, odezva; bez cesty ji nejdřív zjistí (`write`) |
| POST | `/api/v1/contacts/import-card` | Import vizitky kontaktu `{"card": "meshcore://…"}` (`write`) |
| POST | `/api/v1/contacts/{key}/neighbours` | Sousedé uzlu (`write`) |
| POST | `/api/v1/contacts/{key}/acl` | Oprávnění (ACL) uzlu (`admin`) |
| POST | `/api/v1/contacts/{key}/cli` | CLI příkaz na uzlu, vrátí jeho odpověď (`admin`) |
| GET / PATCH | `/api/v1/settings/contacts` | Ruční přidávání kontaktů, obsazenost companionu (`admin` pro změnu) |
| GET | `/api/v1/commands/{id}` | Stav příkazu ve frontě |
| WS | `/api/v1/ws` | Živé události |

Zprávy obsahují i `hops` – trasu rozdělenou na jednotlivé hopy, pokud ji firmware poskytl. Každý hop má `hash` (začátek public key repeateru, 1–3 bajty podle odesílatele) a `names` – jména repeaterů a room serverů z kontaktů, jejichž klíč tímto hashem začíná. U krátkých hashů jich může být víc, neznámý repeater má prázdný seznam:

```json
"hops": [{"hash": "90bc", "names": ["radyne.meshc"]}, {"hash": "8a33", "names": []}]
```

### Odesílání zpráv

```bash
# do kanálu 4
curl -X POST http://server:8080/api/v1/messages \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"kind": "channel", "to": 4, "text": "Ahoj"}'

# DM (public key kontaktu nebo jeho prefix)
curl -X POST http://server:8080/api/v1/messages \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"kind": "dm", "to": "a9caa34c6d6b", "text": "Ahoj"}'
```

Odpověď `202` vrátí uloženou zprávu (`status: pending`) a `command_id`. Zprávy se odesílají postupně přes frontu v databázi, která přežije restart i výpadek companionu. Průběh hlásí WebSocket:

- `pending` → `sent`: zpráva odešla. Kanálové zprávy nemají potvrzení, tím končí.
- `sent` → `acked`: příjemce DM potvrdil doručení.
- U kanálových zpráv server ještě 2 minuty po odeslání počítá, kolikrát companion zaslechl jejich opakování repeatery: `heard_repeats` (počet) a `heard_by` (repeatery jako u `hops`, s jmény z kontaktů). Každé nové opakování ohlásí `message.status`. Je to náhrada potvrzení, že zpráva odešla do sítě; funguje jen se zapnutým `companion.decrypt_channel_logs`.
- `failed`: s důvodem v `error`. Zpráva neodešla do `companion.message_expiry_minutes` (výchozí 10 min) nebo DM nikdo nepotvrdil ani po `companion.dm_max_attempts` pokusech (výchozí 3; poslední jde floodem). Pozdní potvrzení zprávu dodatečně označí `acked`.

Délka textu je omezená firmwarem na 160 bajtů UTF-8 (emoji = 4 B). Do kanálu se odesílá `jméno: text`, proto je tam limit kratší o jméno nodu; delší text API odmítne (firmware by ho tiše ořízl). DM jde jen kontaktu, který je v companionu (`on_device`).

**Region kanálu:** `PATCH /api/v1/channels/4` s `{"region": "#plzen"}` (`"*"` = bez regionu, `null` = výchozí region zařízení). Companion si region kanálu nepamatuje, proto ho drží server a před každou zprávou do kanálu ho nastaví.

**Region kontaktu:** `PATCH /api/v1/contacts/{key}` s `{"region": "#plzen"}` (stejné hodnoty). Server ho nastaví před DM, příspěvkem do room serveru, přihlášením (i automatickým) a požadavky na správu uzlu, a hned potom vrátí výchozí. Uplatní se jen u paketů odeslaných floodem – bez známé trasy, po jejím resetu a u posledního pokusu o doručení DM; zprávy se známou trasou region nepoužívají.

### Kontakty a kanály

Server drží v databázi všechny kontakty, které kdy viděl; companion jich pojme jen omezený počet (`max_contacts`). Když je při přidávání plný, server z companionu odebere nejdéle neaktivní kontakt, který není oblíbený (repeatery a room servery až nakonec); v databázi zůstane s `on_device: false` a jde kdykoli vrátit. DM kontaktu, který v companionu není, ho tam nejdřív přidá (`companion.auto_add_contacts_on_send`).

Kanál: `PUT /api/v1/channels/2` s `{"name": "#plzen", "region": "#cz"}` vytvoří hashtag kanál (klíč se odvodí ze jména). Soukromý kanál potřebuje `secret` (32 hex znaků), např. `{"name": "Rodina", "secret": "…"}`.

Akce správy se provádějí hned; když companion není připojený, API vrátí `503`, když ji companion odmítne, `409`.

### Room servery

Room server je samostatný typ konverzace (`kind: "room"`, v nepřečtených `room:<prefix>`). Příspěvky ostatních přicházejí pod klíčem roomu; skutečného autora server uloží do `author_prefix` (prefix jeho klíče) a jméno autora doplní do `sender_name` z kontaktů (neznámý autor = jeho prefix).

```bash
# přihlášení (prázdné heslo = host); remember=true uloží heslo a po reconnectu se server přihlásí sám
POST /api/v1/contacts/a1b2c3d4e5f6/login   {"password": "heslo", "remember": true}
# příspěvek do roomu
POST /api/v1/messages                       {"kind": "room", "to": "a1b2c3d4e5f6", "text": "Ahoj"}
# historie roomu
GET  /api/v1/messages?kind=room&peer=a1b2c3d4e5f6
```

Přihlášení čeká na odpověď uzlu (podle vzdálenosti sekundy až desítky sekund); server mezitím normálně odesílá a přijímá. Výsledek ohlásí i událost `contact.login`; v seznamu kontaktů mají room servery a repeatery pole `login` (přihlášen, admin, automatické přihlášení). Heslo se ukládá jen s `remember: true`, a to do databáze serveru v čitelné podobě – chraňte proto databázi i zálohy. Do API ani exportu se heslo nikdy nedostane. `logout` ho ve výchozím stavu smaže (`?forget=false` ho ponechá).

### Správa repeaterů a room serverů

Po přihlášení k uzlu (`POST /api/v1/contacts/{key}/login`, pro ACL a CLI jako admin) jde uzel spravovat přes rádio:

| Endpoint | Vrací |
| --- | --- |
| `…/status` | baterie (mV), délka fronty, šum, poslední RSSI a SNR, počty přijatých/odeslaných paketů (flood/direct, duplicity), airtime, uptime |
| `…/telemetry` | telemetrie v Cayenne LPP, např. `[{"channel": 1, "type": "voltage", "value": 4.12}]` |
| `…/neighbours` | sousedé, které uzel slyší přímo: prefix klíče, jméno z kontaktů, SNR, před kolika sekundami |
| `…/acl` | kdo má na uzlu jaká oprávnění, se jmény z kontaktů |
| `…/cli` | `{"command": "get radio"}` → `{"reply": "> 869.525,250.0,11,5"}` |

Každá odpověď obsahuje i `elapsed` – jak dlouho trvala cesta tam a zpět. Požadavek čeká na odpověď podle vzdálenosti uzlu (nejméně 8 s, nejvýš 90 s); server mezitím normálně odesílá a přijímá zprávy. Na jeden uzel jde vždy jen jeden požadavek najednou. Když uzel neodpoví, API vrátí `409` s důvodem (a připomene přihlášení). Odpovědi CLI se neukládají mezi zprávy.

### Sledování uzlů

| Metoda | Cesta | Popis |
|---|---|---|
| GET | `/api/v1/tracked` | Sledované uzly s posledním stavem / telemetrií a příznakem `online` (`read`) |
| PUT | `/api/v1/tracked/{key}` | Začít sledovat uzel, první dotaz proběhne hned (`write`) |
| DELETE | `/api/v1/tracked/{key}` | Přestat sledovat (smaže i historii hodnot) (`write`) |
| POST | `/api/v1/tracked/{key}/poll` | Zeptat se hned (`write`) |
| GET | `/api/v1/tracked/{key}/history?hours=24&fields=bat,battery_percent` | Historie hodnot (`read`) |

Server se sledovaných uzlů ptá postupně, jednou za `tracking.interval_minutes` (výchozí 30 min, nejméně 15 – každý dotaz je paket v síti). Repeater a room server: přihlásí se (uložené heslo z `/login`, jinak jako host) a zeptá se na stav – baterie, uptime, šum, RSSI/SNR, airtime, počty paketů. Klient a senzor: telemetrie (Cayenne LPP), pokud ji uzel povolí – u cizích companionů obvykle jen když je companion v jejich ACL. Po neúspěchu se interval prodlužuje až na `tracking.max_backoff_minutes` (6 h). `online` = úspěšná odpověď v posledních 2,5 intervalu.

Historie (`node_samples`) drží ploché hodnoty: `bat` (mV), `battery_percent`, pole stavu (`noise_floor`, `last_rssi`, `uptime`, `nb_sent`…) a telemetrii jako `ch1_voltage`, `ch2_temperature`. Maže se po `tracking.history_days` (30 dní). Po každém dotazu přijde událost `node.tracked`, po odebrání `node.untracked`.

### Nastavení nodu

```json
PATCH /api/v1/device
{"name": "PiPin89", "lat": 49.75, "lon": 13.38, "tx_power": 20, "default_region": "#cz",
 "radio": {"freq": 869.525, "bw": 250, "sf": 11, "cr": 5}}
```

Posílají se jen měněné položky; `lat` a `lon` vždy spolu, `radio` vždy celé. `"default_region": ""` výchozí region zruší. Server hodnoty předem zkontroluje (výkon do `max_tx_power` zařízení, povolené šířky pásma LoRa, SF 5–12, CR 5–8). **Pozor:** parametry rádia, které se liší od okolní sítě, node od sítě odříznou. Podle verze firmwaru se změna rádia může projevit až po restartu (`POST /api/v1/device/reboot`).

### Nepřečtené zprávy

Server vede stav přečtení pro každého čtenáře zvlášť. Čtenář je vždy vázaný na token (`<token>:<reader>`), takže klient s více uživateli posílá v `reader` ID svého uživatele a uživatelé různých klientů se nepletou.

```bash
GET  /api/v1/unread?reader=petr
POST /api/v1/read  {"conversation": "ch:4", "message_id": 1234, "reader": "petr"}
```

Konverzace jsou `dm:<prefix klíče, 12 hex>` a `ch:<číslo kanálu>`. Značka se posouvá jen dopředu (`"force": true` povolí návrat, tj. „označit jako nepřečtené“). Změnu oznámí událost `read.updated`, ať si počty srovnají i ostatní zařízení téhož uživatele.

### Stav serveru

`GET /api/v1/server` (jen `admin`) vrací:

- `database`: typ a verze DB, velikost, počet řádků a velikost hlavních tabulek, verze schématu
- `load`: CPU a RAM procesu, zatížení systému, délka fronty, zprávy za hodinu a 24 hodin
- `companion`: transport a adresa, stav spojení, reconnecty, poslední příjem a odeslání, odezva posledního příkazu
- `clients`: IP klienta, který se ptá, a připojení WebSocket klienti (IP, token, od kdy)

U SQLite se velikost tabulek čte z virtuální tabulky `dbstat`; kde v SQLite chybí, je vyplněná jen velikost celého souboru (`size_method: "file"`). Za reverse proxy se IP klienta bere z `X-Forwarded-For`, ale jen od adres v `server.trusted_proxies`.

### WebSocket

Připojení `ws://<server>:8080/api/v1/ws?token=<token>` (z prohlížeče místo tokenu `?ticket=<lístek>`). Po připojení přijde `{"type": "hello", "seq": N}` a pak události ve tvaru:

```json
{"seq": 123, "type": "message.received", "ts": "2026-11-11T09:03:19Z", "data": {…}}
```

Typy událostí: `message.received` (příchozí), `message.created` (odchozí zařazená do fronty), `message.status` (změna stavu odchozí), `command.finished`, `contact.updated`, `contact.deleted`, `contacts.synced`, `contact.login`, `read.updated`, `channels.updated`, `advert.received`, `device.updated`, `device.connection`, `device.battery` (napětí a odhad nabití companionu – při změně o 20 mV, jinak nejvýš jednou za 10 minut), `node.tracked`, `node.untracked` (sledované uzly).

Po výpadku spojení se klient připojí s `&since=<poslední seq>` a server mu nejdřív pošle zmeškané události. Pokud už je nemá (drží je `database.events_retention_days`, výchozí 7 dní), pošle `{"type": "resync.required"}` a klient si data načte znovu přes REST. Neplatný token ukončí spojení kódem 4401. Klient může posílat `{"type": "ping"}`, server odpoví `{"type": "pong"}`.

## Instalace přes pip

```bash
python3 -m venv venv
. venv/bin/activate
pip install .
meshcore-server run
```

### Jako služba (systemd)

```bash
useradd --system --home /var/lib/meshcore-server --create-home --shell /usr/sbin/nologin meshcore
python3 -m venv /opt/meshcore-server/venv
/opt/meshcore-server/venv/bin/pip install .
mkdir -p /etc/meshcore-server
cp config.example.yaml /etc/meshcore-server/config.yaml   # a upravit
cp deploy/meshcore-server.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now meshcore-server
journalctl -u meshcore-server -f
```

## Instalace přes Docker

```bash
cp config.example.yaml config.yaml   # a upravit
mkdir -p data && sudo chown 1000:1000 data   # server v kontejneru běží jako UID 1000
docker compose up -d --build
docker compose logs -f
docker compose exec meshcore-server meshcore-server token create admin --scope admin
```

Stav kontejneru včetně healthchecku ukáže `docker compose ps` (sloupec STATUS → `healthy`).

Bez konfiguračního souboru stačí nastavit proměnné prostředí v `docker-compose.yml` (např. `MCS_COMPANION__TCP__HOST`). V tom případě odstraňte řádek s připojením `config.yaml`, jinak Docker na hostiteli vytvoří prázdný adresář tohoto jména.

BLE a USB serial v Dockeru vyžadují přístup k hardwaru hostitele; pro ně je jednodušší instalace přes pip a systemd.

## Vývoj bez rádia

`tools/fake_companion.py` je jednoduchý falešný companion přes TCP. Odpovídá na základní příkazy a v zadaném intervalu „přijme“ zprávu do kanálu Public:

```bash
python tools/fake_companion.py --port 5000 --interval 20
MCS_COMPANION__TCP__HOST=127.0.0.1 meshcore-server run
```

Nová migrace po změně `db/schema.py` (pro vývojáře) se vygeneruje z Pythonu proti dočasné SQLite databázi:

```python
from alembic.config import Config
from alembic import command
cfg = Config()
cfg.set_main_option("script_location", "src/meshcore_server/db/migrations")
cfg.set_main_option("sqlalchemy.url", "sqlite:////tmp/migrace.db")
command.upgrade(cfg, "head")
command.revision(cfg, message="popis změny", autogenerate=True)
```

## Licence

MIT, viz [LICENSE](LICENSE).
