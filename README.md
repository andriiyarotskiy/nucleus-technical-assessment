# Transaction Event Service

Python foundation for the asynchronous transaction processing service described
in [docs/architecture.md](docs/architecture.md). The database layer and currency
conversion service are implemented; API endpoints and queue consumption are not
implemented yet.

## Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker with Docker Compose

## Local Commands

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

## Run

```bash
docker compose up --build
```

The API container listens on `http://localhost:8000`. No business routes are
available in this scaffolding phase.

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
