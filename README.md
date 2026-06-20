# iot-otel

A single Go service that ingests high-resolution sensor data from devices over
gRPC, persists it to PostgreSQL, serves realtime + historical charts directly to
the browser, and exports low-resolution OpenTelemetry metrics. It replaces a
Kotlin/Spring + RabbitMQ backend; the Postgres schema and the Python
`db_optimizer` (Douglas–Peucker visual downsampling + 1-week retention) are
reused unchanged.

**Design split:** Postgres is the high-resolution, lossless local store powering
zoomable charts (the weighting already preserves the exact moment conditions
change); OTel metrics are the low-resolution envelope for automation/alerting.

## Architecture

```
Pi device (Python: Adafruit SCD30/SGP40 + device-side timestamps)
   │  gRPC client-stream, Bearer device-token in metadata
   ▼
┌──────────────── iotd (single Go binary) ────────────────┐
│ gRPC ingest ─► auth interceptor ─► batch writer ─► Postgres
│      └─► in-memory hub ─┬─► browser WebSocket (/charts/{id}/realtime)
│                         └─► OTel aggregator ─► OTLP (metrics)
│ HTTP: OAuth2 + sessions; charts (ECharts) + /partial; devices HTMX CRUD
└──────────────────────────────────────────────────────────┘
        ▲                                   ▲
   Postgres  ◄── db_optimizer (Python: RDP weights + retention)
```

The in-process hub replaces RabbitMQ's fan-out. Single instance; in-flight data
is lost on crash (accepted trade-off — Postgres is the source of truth).

## Components

| Path | What |
|---|---|
| `server/cmd/iotd` | the server binary (gRPC + HTTP) |
| `server/api/proto`, `server/api/gen` | gRPC contract + generated Go stubs |
| `server/internal/{config,storage,auth,ingest,hub,sensors,charts,devices,realtime,metrics,web}` | server packages |
| `device/` | Python device client + simulator (gRPC stubs committed) |
| `db_optimizer/` | Python downsampling/retention worker (reused unchanged) |
| `db/*.sql` | schema + dev seed + sessions table |

## Configuration

See `.env.example`. Key variables:

| Var | Default | Purpose |
|---|---|---|
| `GRPC_ADDR` / `HTTP_ADDR` | `:50051` / `:8080` | listen addresses |
| `DB_HOST`/`DB_PORT`/`DB_USER`/`DB_PASSWORD`/`DB_NAME` | localhost/5432/user/secret/fileserver | DB (or set `DATABASE_URL`) |
| `BATCH_MAX_SIZE` / `BATCH_MAX_LATENCY` / `BATCH_QUEUE_CAP` | 500 / 500ms / 4096 | write batching + backpressure |
| `OAUTH_GITHUB_CLIENT_ID` / `_SECRET` / `OAUTH_REDIRECT_URL` | — | GitHub login |
| `COOKIE_SECURE` | false | set true behind HTTPS |
| `WS_ALLOWED_ORIGINS` | same-origin | comma-separated WebSocket origins |
| `DEV_LOGIN_USER_ID` | 0 (off) | dev-only `/dev-login` bypass |
| `GRPC_TLS_CERT_FILE` / `GRPC_TLS_KEY_FILE` | — | enable gRPC TLS (recommended in prod) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` + standard `OTEL_*` | unset = metrics off | metrics destination |

## Build & test

```sh
cd server
go build ./...
go test ./...        # unit + in-process gRPC e2e + httptest handlers (no Docker)
```

Regenerate stubs after editing the proto (`buf` + `protoc-gen-go*` on PATH):

```sh
(cd server && buf generate)                                           # Go
device/.venv/bin/python -m grpc_tools.protoc -I server/api/proto/ingest/v1 \
  --python_out=device/src --grpc_python_out=device/src ingest.proto   # Python
```

## Run

**Server stack** (Postgres + iotd + db_optimizer):

```sh
docker compose up --build -d
```

Local dev without OAuth — enable the dev login bypass:

```sh
(cd server && DEV_LOGIN_USER_ID=1 go run ./cmd/iotd)   # against `docker compose -f docker-compose.dev.yml up -d`
# browse http://localhost:8080/dev-login  (logs in as the seeded device's owner)
```

**Device** (on the Pi): set `TARGET` + `BEARER` (a token created in the web UI), then

```sh
docker compose -f docker-compose.device.yml up --build -d
```

A hardware-free simulator is available for testing the server without a Pi:

```sh
TARGET=localhost:50051 BEARER=<token> device/.venv/bin/python device/src/sim.py
```

## Verification notes

- Data path verified end-to-end against Postgres: simulator → gRPC → rows with
  device timestamps and ascending ids; `db_optimizer` populates
  `sensor_data_weights`.
- Web path verified: authed `/devices`, `/charts/{id}` + `/partial` (ownership
  enforced; 401 without a session), realtime WebSocket, static assets.
- Metrics: enable by pointing `OTEL_EXPORTER_OTLP_ENDPOINT` at a collector /
  Grafana Alloy / Grafana Cloud; `sensor.*` and `iot.*` series will appear.
