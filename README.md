# iot-otel

A single Go service that ingests high-resolution sensor data over gRPC, persists
it to PostgreSQL, serves realtime + historical charts directly to the browser,
and exports low-resolution OpenTelemetry metrics. It replaces the previous
Kotlin/Spring + RabbitMQ backend; the Postgres schema and the Python
`db_optimizer` (Douglas–Peucker visual downsampling + 1-week retention) are
reused unchanged.

See the full plan at `~/.claude/plans/okay-good-pushback-...md`.

## Status

**Phase 1 — data spine (device → gRPC → DB): implemented.**
- gRPC `IngestService` (client-streaming) with a bearer-token auth interceptor
  (`access_tokens`, `valid_until` enforced, short TTL cache).
- Batched multi-row writes to `sensor_data` with size/latency flush and bounded
  backpressure (`internal/storage`).
- Python device client (`device/src/app.py`) switched from WebSocket to gRPC with
  device-side timestamps, a bounded queue, and reconnect; plus a hardware-free
  simulator (`device/src/sim.py`).

Later phases (realtime hub + WS, historical charts, OAuth2/sessions + device UI,
OTel metrics, packaging) are not yet built.

## Layout

```
api/proto/ingest/v1/ingest.proto   gRPC contract
api/gen/...                        generated Go stubs (committed)
cmd/iotd/                          the binary (Phase 1: gRPC listener)
internal/{config,storage,auth,ingest,model}/
device/src/{app.py,sim.py}         Python device client + simulator (gRPC stubs committed)
db/*.sql                           reused schema (+ dev-only seed)
docker-compose.dev.yml             local Postgres for verification
```

## Configuration (env)

| Var | Default | Purpose |
|---|---|---|
| `GRPC_ADDR` | `:50051` | gRPC listen address |
| `DATABASE_URL` | built from `DB_*` | pgx connection string |
| `DB_HOST`/`DB_PORT`/`DB_USER`/`DB_PASSWORD`/`DB_NAME` | `localhost`/`5432`/`user`/`secret`/`fileserver` | used if `DATABASE_URL` unset |
| `BATCH_MAX_SIZE` | `500` | flush when buffer reaches this many rows |
| `BATCH_MAX_LATENCY` | `500ms` | flush at least this often |
| `BATCH_QUEUE_CAP` | `4096` | bounded ingest queue (backpressure) |

## Build & test

```sh
go build ./...
go test ./...        # unit + in-process gRPC end-to-end (no Docker needed)
```

Regenerate stubs after editing the proto (`buf` + `protoc-gen-go*` on PATH):

```sh
buf generate                                                   # Go stubs
device/.venv/bin/python -m grpc_tools.protoc -I api/proto/ingest/v1 \
  --python_out=device/src --grpc_python_out=device/src ingest.proto   # Python stubs
```

## End-to-end verification (requires Docker)

```sh
# 1. Postgres with schema + a dev device/token (token: devtoken000000000000000000000000)
docker compose -f docker-compose.dev.yml up -d

# 2. Run the ingest service
go run ./cmd/iotd

# 3. Stream synthetic readings from the simulator
TARGET=localhost:50051 BEARER=devtoken000000000000000000000000 \
  device/.venv/bin/python device/src/sim.py

# 4. Confirm rows land with device-side timestamps and ascending id
docker compose -f docker-compose.dev.yml exec db \
  psql -U user -d fileserver -c \
  "select id, sensor_name, sensor_value, received_at from sensor_data order by id desc limit 8;"
```

To confirm the `db_optimizer` invariant holds, point the existing
`../iot/db_optimizer` at this database and verify it populates
`sensor_data_weights` without error.
