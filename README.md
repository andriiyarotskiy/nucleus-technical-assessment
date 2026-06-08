# Transaction Event Service

Async transaction processing service built with FastAPI, Redis Streams,
PostgreSQL, and SQLAlchemy. Detailed design and failure handling are documented
in [docs/architecture.md](docs/architecture.md).

## Run

Requires Docker with Docker Compose:

```bash
git clone git@github.com:andriiyarotskiy/nucleus-technical-assessment.git
```

```bash
cd nucleus-technical-assessment
```

```bash
docker compose up --build
```

The committed `.env.example` contains local-only defaults used by Docker
Compose, so no setup step is required. The API is available at
`http://localhost:8000`, with interactive docs at `http://localhost:8000/docs`.

## Project Structure

```text
app/
├── api/             FastAPI routes and dependencies
├── core/            configuration and logging
├── db/              SQLAlchemy models, sessions, and repositories
├── schemas/         API and queue contracts
├── services/        business logic and integration services
├── worker/          Redis Streams consumer and worker entrypoint
└── main.py          FastAPI application wiring
```

The packages follow runtime responsibilities. The transaction repository is the
only persistence abstraction because it owns actual SQLAlchemy queries and
transaction boundaries; services contain business decisions, while routes and
the worker contain orchestration.

## Verification

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/):

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy app
docker compose config
docker compose up --build
```

## Why Redis Streams

Redis Streams provides consumer groups, acknowledgements, and pending-message
recovery while keeping the local stack small. It supports the required
at-least-once delivery model without introducing a separate message broker.

## Delivery And Retry

- `POST /events` returns `202 Accepted` only after `XADD` succeeds.
- The worker persists the transaction first and sends `XACK` only after a
  successful database write or a confirmed duplicate no-op.
- Retryable failures such as PostgreSQL or rate-provider errors are left
  unacknowledged in the consumer group's pending list.
- The worker inspects pending entries and retries them with capped backoff:
  `1s`, `2s`, `4s`, `8s`, then up to `30s`.
- Invalid payloads and unsupported currencies are written to the dead-letter
  stream and only then acknowledged.

## Idempotency And Deduplication

- The event `id` is the PostgreSQL primary key for `transactions`.
- The worker uses `INSERT ... ON CONFLICT DO NOTHING`, so duplicate delivery
  does not create duplicate rows.
- This is an at-least-once system: the same message can be processed more than
  once, but persisted transactions remain idempotent by event ID.

## Trade-off

Currency rates are fixed in memory instead of fetched from an external provider.
This keeps the assessment deterministic and easy to run, but rates are not
current. The rate-provider boundary allows an external service to replace the
fixed implementation later.

## At 10x Load

At sustained 10x load, I would add worker replicas, increase stream batch sizes,
partition work across streams if one stream became a bottleneck, and introduce
cached external exchange rates. I would also validate PostgreSQL capacity with
load tests and add read replicas or precomputed summaries only if measurements
showed they were needed.

## Repository

https://github.com/andriiyarotskiy/nucleus-technical-assessment
