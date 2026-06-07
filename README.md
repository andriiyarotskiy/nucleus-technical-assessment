# Transaction Event Service

Python foundation for the asynchronous transaction processing service described
in [docs/architecture.md](docs/architecture.md). The database layer, currency
conversion service, event ingestion endpoint, read APIs, and core Redis Streams
worker are implemented.

## Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker with Docker Compose

## Local Commands

Create the local environment file before running the application:

```bash
cp .env.example .env
```

`.env.example` contains safe local defaults and is committed as a template.
`.env` is ignored by Git and is the single source of environment values used by
Docker Compose and `pydantic-settings`. Replace the local credentials before
using the configuration outside local development.

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy app
docker compose up --build
```

## Database

PostgreSQL stores one `transactions` table. The source event UUID is the primary
key for durable deduplication. Decimal columns preserve money and rate precision,
check constraints reject invalid stored values, and the composite
`(user_id, event_timestamp DESC, id DESC)` index supports filtered transaction
listing and per-user aggregation.

Apply or inspect migrations with:

```bash
uv run alembic upgrade head
uv run alembic downgrade base
uv run alembic upgrade head --sql
```

Docker Compose runs the migration as a one-shot service before starting the API.

## Currency Conversion

The in-memory provider uses fixed USD multipliers: USD `1.00`, EUR `1.08`, and
GBP `1.27`. Conversion uses `Decimal` and rounds USD to two decimal places with
`ROUND_HALF_UP`. The converter depends on an async rate-provider protocol so a
future external provider can report temporary failures for worker retries.

## Event Ingestion

`POST /events` validates a transaction event and appends a versioned JSON payload
to the Redis Stream named `transactions`. It returns `202 Accepted` only after
Redis confirms `XADD`, and returns `503 Service Unavailable` when publishing
fails. The endpoint does not write to PostgreSQL.

```bash
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "11111111-1111-1111-1111-111111111111",
    "user_id": "22222222-2222-2222-2222-222222222222",
    "amount": "100.25",
    "currency": "EUR",
    "timestamp": "2026-06-07T12:00:00Z"
  }'
```

## Run

```bash
docker compose up --build
```

The API container listens on `http://localhost:8000`.

## Read APIs

Stored transactions are available through:

```text
GET /users/{user_id}/summary
GET /users/{user_id}/transactions?from=&to=&page=&limit=
```

The list endpoint accepts optional timezone-aware inclusive timestamps, defaults
to page 1 with 50 items, caps `limit` at 100, and orders by event timestamp
descending with ID as a deterministic tie-breaker.

## Worker

The worker runs as `python -m app.worker` and uses the
`transaction-processors` Redis consumer group. It checks whether the transaction
ID is already stored before rate lookup, converts new events to USD, and performs
a race-safe PostgreSQL `INSERT ... ON CONFLICT (id) DO NOTHING RETURNING id`.

Redis messages are acknowledged only after the database transaction exits
successfully or an existing transaction is confirmed. Temporary processing
failures are logged and left pending.

### Delivery Guarantee

The worker provides **at-least-once delivery with idempotent database
processing**:

- New work is read through the `transaction-processors` consumer group.
- Failed work is not acknowledged.
- Due pending messages are reclaimed with `XAUTOCLAIM` after five seconds.
- Temporary failures remain pending and are retried until they succeed.
- Invalid payloads and unsupported currencies are appended to
  `transactions:dead-letter` before the original is acknowledged.

A crash after PostgreSQL commits but before `XACK` causes redelivery. The
transaction ID primary key makes that retry a duplicate instead of a second
stored row. The DLQ append and acknowledgement are separate commands: a crash
between them can duplicate a DLQ entry, but the original payload is not lost.
`source_id` is the DLQ idempotency key.

## Metrics

`GET /metrics` returns Prometheus-style text without requiring a Prometheus
server:

```text
events_processed_total 0
events_failed_total 0
events_duplicate_total 0
```

The worker stores these shared counters in Redis so the API can expose values
from the separate worker process. They are operational, best-effort metrics;
PostgreSQL remains the source of truth for transaction data.

## Design Notes

The following sections will be completed as their implementation slices are
added:

- Why Redis Streams was selected.
- At-least-once delivery and idempotency.
- One accepted trade-off.
- Failure and retry behavior.
- What changes at 10x load.
- Example API requests.
- Public repository URL.
