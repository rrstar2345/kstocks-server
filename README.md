# kstocks-server

Standalone Rust backend that connects to NSE's WebSocket streams for index
and F&O option data, persists raw ticks to SQLite, aggregates them into
OHLC bars, purges old data on a retention schedule, and serves the
aggregated bars over a read-only, per-client-authenticated HTTP API.

## What it does

1. **Ingest** — Connects to NSE's indices and option-chain WSS feeds,
   batches incoming ticks, and writes them to SQLite (`index_ticks` /
   `option_ticks`).
2. **Aggregate** — Rolls raw ticks up into 1-minute OHLC bars every few
   minutes, and 1-minute bars up into daily bars once per day after
   market close.
3. **Retain** — Purges raw ticks and intermediate OHLC tiers once they're
   safely aggregated and outside their retention window; runs a weekly
   `VACUUM`.
4. **Serve** — Exposes the aggregated OHLC bars (never raw ticks, never
   the in-progress candle) over a small read-only HTTP API, gated behind
   per-client API keys.
5. **Authenticate** — Clients self-register, an admin reviews and
   approves/declines/revokes them, and every subsequent request (OHLC
   data or a launch-time validation check) is checked against that
   approval state.

## Architecture

The process is a single binary running several independent background
tasks around one SQLite file, plus an HTTP server. There's no message
queue or external service — everything communicates through the
database (and one in-process `mpsc` channel per tick writer).

```mermaid
flowchart TB
    subgraph NSE["NSE (external)"]
        WSS_IDX["Indices WSS feed"]
        WSS_OPT["Option-chain WSS feed"]
        REST["NSE REST endpoints<br/>(symbols, expiry, server time)"]
    end

    subgraph Ingest["Ingest — market/streamers"]
        STREAM_IDX["indices streamer"]
        STREAM_OPT["options streamer"]
    end

    subgraph Writers["Batched writers — storage/ticks.rs"]
        CH_IDX["mpsc channel"]
        CH_OPT["mpsc channel"]
        W_IDX["index tick writer"]
        W_OPT["option tick writer"]
    end

    subgraph DB["SQLite (single file, WAL mode)"]
        RAW_IDX[("index_ticks")]
        RAW_OPT[("option_ticks")]
        OHLC_IDX_1M[("index_ohlc_1m")]
        OHLC_OPT_1M[("option_ohlc_1m")]
        OHLC_IDX_1D[("index_ohlc_1d")]
        WATERMARK[("aggregation_state")]
    end

    subgraph Agg["Aggregation — storage/ohlc.rs"]
        AGG_1M["1-minute aggregation job<br/>(every run_interval_secs)"]
        ROLLUP["daily 1m→1d rollup<br/>(once/day, 16:15 IST)"]
    end

    subgraph Ret["Retention — storage/retention.rs"]
        PURGE["daily purge<br/>(once/day, 16:30 IST)"]
        VACUUM["weekly VACUUM"]
    end

    subgraph API["Read-only HTTP API — api/"]
        POOL["dedicated SqlitePool<br/>(busy_timeout, separate from writers)"]
        EP_IDX["GET /ohlc/index"]
        EP_OPT["GET /ohlc/option"]
        EP_HEALTH["GET /health"]
    end

    CONSUMER["API consumer<br/>(e.g. desktop app)"]

    WSS_IDX --> STREAM_IDX --> CH_IDX --> W_IDX --> RAW_IDX
    WSS_OPT --> STREAM_OPT --> CH_OPT --> W_OPT --> RAW_OPT
    REST -.-> STREAM_IDX
    REST -.-> STREAM_OPT

    RAW_IDX --> AGG_1M
    RAW_OPT --> AGG_1M
    AGG_1M --> OHLC_IDX_1M
    AGG_1M --> OHLC_OPT_1M
    AGG_1M <--> WATERMARK

    OHLC_IDX_1M --> ROLLUP --> OHLC_IDX_1D
    ROLLUP <--> WATERMARK

    WATERMARK -.gates.-> PURGE
    RAW_IDX -.-> PURGE
    RAW_OPT -.-> PURGE
    OHLC_IDX_1M -.-> PURGE
    OHLC_OPT_1M -.-> PURGE
    OHLC_IDX_1D -.-> PURGE
    PURGE --> VACUUM

    OHLC_IDX_1M --> POOL
    OHLC_IDX_1D --> POOL
    OHLC_OPT_1M --> POOL
    RAW_IDX -.status only.-> POOL
    RAW_OPT -.status only.-> POOL
    WATERMARK -.status only.-> POOL
    POOL --> EP_IDX --> CONSUMER
    POOL --> EP_OPT --> CONSUMER
    POOL --> EP_HEALTH --> CONSUMER
```

Key architectural points:

- **Two SQLite pools, one file.** The ingest writers and the HTTP API use
  separate `SqlitePool`s against the same database file. The API's pool
  sets a `busy_timeout`, so a slow analytical read never blocks — or gets
  blocked by — the high-frequency tick writers under WAL mode.
- **Everything downstream is watermark-driven.** Aggregation only scans
  ticks newer than `aggregation_state.last_bucket_end`; retention only
  purges data that aggregation has already confirmed it processed. There's
  no shared lock or coordinator — each job reads/writes its own watermark
  row and moves forward independently.
- **No live/streaming path through the API.** The API only ever reads
  already-committed OHLC rows. A consumer wanting the in-progress candle
  is expected to connect to NSE's WSS directly and use this API only for
  historical gap-fill.
- **Upserts make every stage idempotent.** Aggregation and rollup both use
  `INSERT ... ON CONFLICT ... DO UPDATE`, so a crashed or restarted job can
  safely re-process a bucket without creating duplicates.

## Project layout

```
src/
  main.rs              entry point: wiring, task spawning, shutdown, `admin` CLI subcommand
  settings.rs           config structs + load/save of settings_server.json
  stats.rs               shared in-memory stats (dashboard + health endpoint)
  market/               everything related to fetching NSE data
    http.rs               shared reqwest client + NSE User-Agent
    market_clock.rs        NSE-clock-derived session mode (Active/Idle)
    symbols.rs             resolves F&O symbols + nearest expiry
    streamers/
      indices.rs            indices WSS streamer
      options.rs             option-chain WSS streamer
  storage/              persistence: ingest, aggregation, retention
    ticks.rs              schema, batched tick writers
    ohlc.rs                1m/1d OHLC aggregation (watermark-driven)
    retention.rs           purge + vacuum routines
  users/                client registration/auth, in its own kstocks-users.db
    mod.rs                 pool init, schema, client/admin-token queries
    keys.rs                client key + admin token generation/hashing
    admin_cli.rs           `kstocks-server admin generate|regenerate` logic
  api/                  read-only HTTP API + auth
    mod.rs                 router, shared state, range/interval helpers
    index_ohlc.rs          GET /ohlc/index          (client key required)
    option_ohlc.rs          GET /ohlc/option          (client key required)
    health.rs               GET /health
    register.rs             POST /register
    validate.rs             GET /validate            (client key required)
    client_auth.rs          shared client-key parsing/lookup logic
    auth_middleware.rs      middleware enforcing client key on /ohlc/*
    admin.rs                GET/POST /admin/*        (admin token required)
  utils/
    dashboard.rs           terminal dashboard (ratatui)
```

## Running

```
cargo run -- [--no-dashboard]
```

`--no-dashboard` runs headless (useful under systemd/cron); otherwise an
interactive terminal dashboard shows stream/db/session status until you
quit it (streaming continues in the background either way).

On first run, a default config is written to
`<data-local-dir>/.kstocks/settings_server.json` (falls back to the
current directory if no data-local-dir is available). Edit that file to
change ports, retention windows, aggregation cadence, etc. — see
[Configuration](#configuration) below.

### Admin token setup

`/admin/*` routes (reviewing and approving/declining/revoking clients)
require a bearer token that can **only** be minted from the machine the
server runs on — there is no HTTP endpoint that creates or rotates it.
This is a one-shot CLI mode; it does not start the server or streamers.

```
kstocks-server admin generate      # first-time setup; fails if a token already exists
kstocks-server admin regenerate    # rotate — invalidates the previous token immediately
```

Each prints the plaintext token to stdout **once**. Store it securely
(e.g. a password manager or secrets file with restricted permissions) —
it cannot be recovered later, only rotated. The running server checks
incoming `/admin/*` requests against the hash stored in
`kstocks-users.db`, so no restart is needed after `regenerate` — the very
next admin request is validated against the new token.

## Configuration

All settings live in `settings_server.json`, created with defaults on
first run. Relevant sections for this feature set:

```jsonc
{
  "aggregation": {
    "run_interval_secs": 300     // how often the 1m OHLC job runs
  },
  "retention": {
    "raw_ticks_keep_trading_days": 2,
    "index_ohlc_1m_keep_days": 60,
    "option_ohlc_1m_expiry_grace_days": 7,
    "index_ohlc_1d_keep_days": 1095   // 3 years; 0 = keep forever
  },
  "api": {
    "port": 8787
  }
}
```

| Field | Default | Meaning |
|---|---|---|
| `aggregation.run_interval_secs` | `300` | How often (seconds) raw ticks are rolled up into `index_ohlc_1m` / `option_ohlc_1m`. The daily `index_ohlc_1m` → `index_ohlc_1d` rollup runs once per day at 16:15 IST, independent of this setting. |
| `retention.raw_ticks_keep_trading_days` | `2` | Raw `index_ticks` / `option_ticks` older than this are purged, gated on confirmed `*_ohlc_1m` coverage. |
| `retention.index_ohlc_1m_keep_days` | `60` | `index_ohlc_1m` rows older than this are purged, gated on confirmed `index_ohlc_1d` coverage. |
| `retention.option_ohlc_1m_expiry_grace_days` | `7` | `option_ohlc_1m` rows are purged once `expiry_date < today - N days`. Hard rule based on contract expiry, not a rolling window. |
| `retention.index_ohlc_1d_keep_days` | `1095` (3 yrs) | `index_ohlc_1d` rows older than this are purged. `0` = keep indefinitely. |
| `api.port` | `8787` | Port the read-only HTTP API listens on (all interfaces). |

The daily purge runs once per day at 16:30 IST (after the daily rollup),
followed by `PRAGMA optimize`. `VACUUM` runs weekly on its own schedule
since it briefly locks the whole database file.

## Data model

### Raw ticks
- `index_ticks` — one row per index tick as received from NSE.
- `option_ticks` — one row per option-chain tick (CE/PE combined, wide
  shape).

### OHLC tiers
- `index_ohlc_1m` — 1-minute index bars: `(index_name, bucket_start)` →
  `open, high, low, close, tick_count`.
- `index_ohlc_1d` — daily index bars, rolled up from `index_ohlc_1m`
  (never from raw ticks). Same shape as `index_ohlc_1m`.
- `option_ohlc_1m` — 1-minute option bars, wide CE/PE shape:
  `(symbol, expiry, strike_price, bucket_start)` →
  `ce_open/high/low/close/volume/oi_close`, `pe_open/high/low/close/volume/oi_close`,
  `tick_count`, plus a comparable `expiry_date` column for cheap retention
  checks. There is no `option_ohlc_1d` — options don't need daily bars.

### Bookkeeping
- `aggregation_state` — one row per aggregated table
  (`index_ohlc_1m` / `option_ohlc_1m` / `index_ohlc_1d`), tracking
  `last_bucket_end` so each aggregation run only scans new data.

### Aggregation guarantees
- **Idempotent**: every insert is an upsert
  (`INSERT ... ON CONFLICT ... DO UPDATE`), so re-running a pass over the
  same range never duplicates rows.
- **No partial bars**: only fully-elapsed 1-minute buckets are ever
  aggregated; the in-progress current minute is always excluded.
- **No gap-filling**: a bucket with zero raw ticks (genuine lull or WSS
  outage — not distinguished) simply has no row. No nulls, no
  fill-forward.

### Users database (`kstocks-users.db`)

Client registration and auth state live in a **separate** SQLite file
from market data, so the two never share a writer, a backup policy, or a
failure mode.

- `clients` — one row per registered username: `key_id`, `secret_hash`
  (never the plaintext secret), `status` (`pending` / `approved` /
  `declined` / `revoked`), `registered_ip`, timestamps. A `revoked` row is
  never deleted (so future telemetry keyed on the numeric `id` isn't
  orphaned) and is instead overwritten in place if that username
  registers again.
- `admin_token` — single row holding the current admin token's hash. Only
  ever written by the `admin generate`/`admin regenerate` CLI subcommand
  (see [Admin token setup](#admin-token-setup)), never by any HTTP route.

## HTTP API

Read-only for market data. Runs on its own SQLite connection pool
(separate from the ingest writers, same database file) with a
`busy_timeout`, so slow reads never contend with the ingest writer under
WAL mode. Client and admin auth state is read from the separate
`kstocks-users.db` (see [Users database](#users-database-kstocks-usersdb)).

The API only ever serves **completed, already-aggregated** bars from the
OHLC tables. It never reads raw ticks and never represents the
in-progress current candle — that's the desktop app's job once it
connects directly to NSE's WSS for gap-fill. There is intentionally no
push/streaming endpoint.

Base URL: `http://<host>:<api.port>` (default port `8787`).

### Authentication model

Every client (e.g. each install of the desktop app) has a single API key
of the form:

```
<username>-<key_id>-<secret>
```

e.g. `johndoe-adc214s3-2jfh79gs`. Sent on every authenticated request as:

```
Authorization: Bearer <username>-<key_id>-<secret>
```

**Lifecycle:**

1. Client calls `POST /register` once, choosing a username. The server
   generates the key pair server-side and returns the **full plaintext
   key in that single response** — the client must persist it locally
   (e.g. OS keychain or a local config file) and never display it to the
   user. The server only ever stores a hash of the secret half, never the
   plaintext.
2. The registration starts in `pending` status. It is **not yet usable**
   against `/ohlc/*` — the key exists but is dormant until an admin
   approves it via `/admin/*`.
3. On every app launch (and periodically thereafter, if desired), the
   client calls `GET /validate` with its stored key to check current
   status before showing its main screen.
4. An admin can `decline` a pending request, or `revoke` an already
   `approved` one at any time — both immediately block `/ohlc/*` and
   `/validate` on the next request, no client-side action needed.

**Endpoint auth summary:**

| Endpoint | Auth required |
|---|---|
| `POST /register` | None (rate-limited instead — see below) |
| `GET /validate` | Client key (any status — returns the status itself) |
| `GET /ohlc/index`, `GET /ohlc/option` | Client key, **must be `approved`** |
| `GET /health` | None |
| `/admin/*` | Admin token (see [Admin token setup](#admin-token-setup)) |

A client key that is `pending`, `declined`, or `revoked` is rejected from
`/ohlc/*` with the same `401 Unauthorized` as a malformed or unknown key
— the response never reveals *why* a key doesn't work there, to avoid
leaking which usernames/key_ids exist. `/validate` is the one place a
non-approved client learns its actual status, by design, so the desktop
app can show a meaningful "pending approval" / "access revoked" message.

**Registration rate limiting**, to bound abuse of the open `/register`
endpoint:
- One registration attempt per IP per rolling 24h window is the target
  behavior in most cases, but the actual enforced rules are:
  - A username with an existing `pending`, `approved`, or `declined` row
    cannot register again (`400 Bad Request`) — only a `revoked` username
    may re-register, which overwrites its row with a fresh key pair.
  - An IP that has produced 5 or more registrations (any username) in the
    last 24h is throttled (`429 Too Many Requests`), regardless of
    username status.
- Dormant `pending` keys carry no access on their own (see auth summary
  above), so this limiter's job is bounding review-queue noise and DB
  growth, not preventing unauthorized data access.

---

### `GET /ohlc/index`

Requires `Authorization: Bearer <client key>` with `status: approved`
(see [Authentication model](#authentication-model)).

Index OHLC bars, sourced from `index_ohlc_1m` or `index_ohlc_1d`
depending on the requested interval.

**Query parameters**

| Param | Required | Description |
|---|---|---|
| `name` | yes | Index name (as stored in `index_ticks.index_name`) |
| `range` | yes | One of `1d, 3d, 5d, 7d, 14d, 1mo, 3mo, 6mo, 1y` |
| `interval` | yes | Must be valid for the given `range` (table below) |

**Valid `range` → `interval` combinations**

| `range` | Valid `interval` values | Source table |
|---|---|---|
| `1d` | `1m, 3m, 5m, 15m, 30m` | `index_ohlc_1m` |
| `3d` | `15m, 30m, 1h` | `index_ohlc_1m` |
| `5d` | `30m, 1h, 2h` | `index_ohlc_1m` |
| `7d` | `1h, 2h, 4h` | `index_ohlc_1m` |
| `14d` | `2h, 4h` | `index_ohlc_1m` |
| `1mo` | `4h` (from `index_ohlc_1m`), `1d` (from `index_ohlc_1d`) | mixed |
| `3mo` | `1d, 1w` | `index_ohlc_1d` |
| `6mo` | `1d, 1w` | `index_ohlc_1d` |
| `1y` | `1w, 1mo` | `index_ohlc_1d` |

Any other `range`/`interval` combination returns `400 Bad Request`.

Intervals coarser than the source tier's native bucket size (e.g. `2h`
from 1-minute bars) are aggregated on read via a windowed `GROUP BY`.
Buckets with no underlying data are omitted from the response — gaps are
never filled or synthesized.

**Example request**

```
GET /ohlc/index?name=NIFTY%2050&range=1d&interval=5m
```

**Example response**

```json
[
  {
    "bucket_start": "2026-07-20T03:45:00+00:00",
    "open": 24812.35,
    "high": 24830.10,
    "low": 24805.00,
    "close": 24821.75
  },
  {
    "bucket_start": "2026-07-20T03:50:00+00:00",
    "open": 24821.75,
    "high": 24845.60,
    "low": 24818.90,
    "close": 24840.20
  }
]
```

**Error response** (invalid range/interval)

```json
{ "error": "invalid interval '10m' for range '1d'; valid: [\"1m\", \"3m\", \"5m\", \"15m\", \"30m\"]" }
```

---

### `GET /ohlc/option`

Requires `Authorization: Bearer <client key>` with `status: approved`
(see [Authentication model](#authentication-model)).

Option OHLC bars (wide CE/PE shape), always sourced from
`option_ohlc_1m`.

**Query parameters**

| Param | Required | Description |
|---|---|---|
| `symbol` | yes | F&O symbol (e.g. `NIFTY`) |
| `expiry` | yes | Expiry string as stored on the tick (e.g. `25-Jul-2026`) |
| `strike` | yes | Strike price (numeric) |
| `range` | yes | One of `1d, 3d, 5d, 7d, 14d` |
| `interval` | yes | Must be valid for the given `range` (table below) |
| `leg` | no | `CE`, `PE`, or `both` (default `both`) |

**Valid `range` → `interval` combinations**

| `range` | Valid `interval` values |
|---|---|
| `1d` | `1m, 5m, 15m` |
| `3d` | `15m, 30m, 1h` |
| `5d` | `30m, 1h, 2h` |
| `7d` | `1h, 2h, 4h` |
| `14d` | `2h, 4h` |

Same gap rule as the index endpoint: missing buckets are omitted, never
filled.

**Example request**

```
GET /ohlc/option?symbol=NIFTY&expiry=25-Jul-2026&strike=25000&range=1d&interval=5m&leg=CE
```

**Example response**

```json
[
  {
    "bucket_start": "2026-07-20T03:45:00+00:00",
    "ce_open": 142.30,
    "ce_high": 145.80,
    "ce_low": 141.10,
    "ce_close": 144.95,
    "ce_volume": 18200,
    "ce_oi_close": 512400,
    "pe_open": null,
    "pe_high": null,
    "pe_low": null,
    "pe_close": null,
    "pe_volume": null,
    "pe_oi_close": null
  }
]
```

When `leg=both`, both `ce_*` and `pe_*` fields are populated (each
independently `null` if that leg had no ticks in the bucket). When
`leg=CE` or `leg=PE`, the other leg's fields are always `null` and a
bucket is only included if the requested leg has data.

**Error response** (invalid leg)

```json
{ "error": "invalid leg; must be CE, PE, or both" }
```

---

### `GET /health`

No query parameters. Returns current ingest/aggregation status.

**Example response**

```json
{
  "db_connected": true,
  "last_index_tick_at": "2026-07-20T09:58:12.481Z",
  "last_option_tick_at": "2026-07-20T09:58:10.902Z",
  "aggregation_watermarks": {
    "index_ohlc_1m": "2026-07-20T09:57:00Z",
    "option_ohlc_1m": "2026-07-20T09:57:00Z",
    "index_ohlc_1d": "2026-07-20T00:00:00Z"
  },
  "session_mode": "ACTIVE"
}
```

| Field | Meaning |
|---|---|
| `db_connected` | Whether the API's read pool could reach the database |
| `last_index_tick_at` / `last_option_tick_at` | Timestamp of the most recent raw tick received, per stream |
| `aggregation_watermarks` | `last_bucket_end` per aggregated table — how far each aggregation tier has processed |
| `session_mode` | `ACTIVE` (holding live WSS connections) or `IDLE` (polling), reusing the same market-hours logic as the dashboard |

---

### `POST /register`

No authentication required (see [rate limiting](#authentication-model)
above). Called once by a new client install.

**Request body**

```json
{ "username": "johndoe" }
```

`username` is sanitized server-side to lowercase alphanumeric characters
only; anything else (spaces, symbols, punctuation) is stripped. If
nothing alphanumeric remains, the request is rejected.

**Example response** (`200 OK`)

```json
{
  "status": "pending",
  "api_key": "johndoe-adc214s3-2jfh79gs"
}
```

`api_key` is shown **exactly once** — store it immediately (e.g. OS
keychain or local config), never re-derivable from the server afterward.
`status` is always `"pending"` on a fresh registration; the key carries
no `/ohlc/*` access until an admin approves it.

**Error responses**

```json
{ "error": "username must contain at least one alphanumeric character" }
```

```json
{ "error": "username already has a registration in 'approved' status" }
```
`400 Bad Request` — returned for any existing `pending`, `approved`, or
`declined` registration under that username. Only a `revoked` username
may register again.

```json
{ "error": "too many registration attempts from this network today" }
```
`429 Too Many Requests` — 5+ registrations from the same IP in the last
24h, regardless of username.

---

### `GET /validate`

Requires `Authorization: Bearer <client key>`. Intended to be called on
every app launch (and optionally on a periodic interval) to decide
whether to show the main screen or a "pending"/"revoked" message.

**Example response** — approved

```json
{ "approved": true, "status": "approved" }
```

**Example response** — not yet approved / declined / revoked

```json
{ "approved": false, "status": "pending" }
```

**Error response** — malformed, unknown, or mismatched key

```json
{ "error": "invalid or unknown key" }
```
`401 Unauthorized`.

---

### `/admin/*`

All admin routes require `Authorization: Bearer <admin token>`, where the
token is generated by the `kstocks-server admin generate` /
`admin regenerate` CLI subcommand (see
[Admin token setup](#admin-token-setup)) — there is no HTTP-level way to
create or rotate this token. Missing/invalid token → `401 Unauthorized`
on every route below.

#### `GET /admin/registrations`

Lists every client, any status, most recently created first.

```json
[
  {
    "id": 4,
    "username": "johndoe",
    "key_id": "adc214s3",
    "status": "pending",
    "registered_ip": "203.0.113.7",
    "created_at": "2026-07-21T05:12:03+00:00",
    "updated_at": "2026-07-21T05:12:03+00:00"
  }
]
```

Note `key_id` is shown (safe — it's a public lookup value, not the
secret), but the secret/secret hash never appears in any admin response.

#### `POST /admin/registrations/{id}/approve`

Moves a `pending` client to `approved`, immediately unlocking `/ohlc/*`
and `/validate` for that client's existing key — no new key is issued.

```json
{ "id": 4, "status": "approved" }
```

#### `POST /admin/registrations/{id}/decline`

Moves a `pending` client to `declined`. The key remains dormant
permanently unless the username is later revoked and re-registers.

```json
{ "id": 4, "status": "declined" }
```

#### `POST /admin/clients/{id}/revoke`

Moves any client (typically `approved`) to `revoked`. Takes effect
immediately — the next `/ohlc/*` or `/validate` call with that key fails.
The row itself is kept (not deleted), so future telemetry tied to `id`
isn't orphaned.

```json
{ "id": 4, "status": "revoked" }
```

All three action endpoints return `404 Not Found` for an unknown `id`:

```json
{ "error": "no such client" }
```

---

## Notes for API consumers

- All timestamps are RFC 3339 / ISO 8601 in UTC.
- Bars are only ever emitted once fully closed — there is no way to get
  the currently-forming candle from this API by design.
- Because gaps are never filled, don't assume evenly-spaced buckets;
  consumers should key off `bucket_start` rather than array index.
- `strike` is compared as a floating-point value against
  `option_ohlc_1m.strike_price` — pass the exact strike as stored (e.g.
  `25000`, not `25000.0` vs `25000.00` formatting concerns; SQLite
  numeric comparison handles this fine).
- Store the client key from `/register` securely and reuse it across app
  launches — it is not retrievable again from the server if lost; the
  only recovery path is an admin revoking and the user re-registering
  under the same or a new username.
- Treat `401 Unauthorized` from `/ohlc/*` as "not currently approved" and
  route the user back through `/validate`'s status rather than assuming
  the key itself is wrong — `pending` and `revoked` both surface as the
  same 401 there, by design (see
  [Authentication model](#authentication-model)).