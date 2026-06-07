# Transaction Event Processing Architecture

## Status

This document is the implementation plan required before coding. No implementation
should begin until this plan is approved.

## Requirements Summary

The service must:

- Accept transaction events over HTTP:
  `{id, user_id, amount, currency, timestamp}`.
- Enqueue accepted events for asynchronous processing.
- Consume events, deduplicate by transaction `id`, convert amounts to USD, and
  persist the result.
- Survive temporary database and currency-rate failures without losing accepted
  events.
- Expose:
  - `POST /transactions`
  - `GET /users/{user_id}/summary`
  - `GET /users/{user_id}/transactions?from=&to=&limit=&offset=`
  - `GET /metrics`
- Run locally with Docker Compose.
- Handle approximately 100 events/second, including short bursts near 1,000
  events/second.
- Include unit tests for deduplication and currency conversion.
- Use Python 3.12, `uv`, `pyproject.toml`, Ruff, mypy, pytest, and
  pytest-asyncio.
- Document local setup, design decisions, verification commands, and the public
  repository URL in `README.md`.

## Assumptions

- Event `id` and `user_id` are UUIDs represented as strings at the API boundary.
- `amount` is positive and supplied as a decimal string or JSON number. It is
  converted immediately to `Decimal`; binary floating-point is not used for money.
- `currency` is a three-letter uppercase ISO 4217 code.
- Event timestamps are timezone-aware. They are normalized to UTC.
- Authentication and authorization are outside this assessment's scope.
- The producer and consumer use the same application codebase but run as separate
  processes.
- PostgreSQL is the durable system of record. Redis is the delivery mechanism,
  not the source queried by read APIs.
- Currency conversion uses an injected `CurrencyRateProvider`. The local
  implementation uses a small configured rate table so Docker Compose works
  without an API key or internet access. The boundary remains capable of using a
  remote provider. The worker treats provider-unavailable errors as retryable;
  this path is tested even though the default local provider has no network
  dependency.
- A configured rate is a multiplier from the source currency to USD. USD has a
  fixed rate of `1`.
- The conversion rate is captured when an event is successfully processed, not
  necessarily at the event's historical timestamp. Historical rates are a
  possible future extension.

## High-Level Architecture

```text
Client
  |
  | POST /transactions
  v
FastAPI API process
  |
  | XADD after validation
  v
Redis Stream: transactions
  |
  | XREADGROUP
  v
Worker process ----> CurrencyRateProvider
  |
  | SQLAlchemy async transaction
  v
PostgreSQL
  ^
  | SELECT aggregate / paginated rows
  |
FastAPI read endpoints

Worker failures remain in the Redis consumer group's pending entries list while
the worker retries with capped exponential backoff. A small reclaim loop recovers
messages abandoned by crashed workers. Non-retryable events are copied to a
dead-letter stream.
```

The application will be divided into a few explicit areas rather than generic
layers:

- HTTP schemas and thin FastAPI routes.
- Transaction ingestion service.
- Transaction processing service containing deduplication and conversion flow.
- Currency conversion logic and rate-provider boundary.
- SQLAlchemy models and focused queries.
- Redis producer, consumer, and retry-loop code.
- Metrics definitions.

No generic repository or base-service framework is planned. SQLAlchemy queries
will live close to the use case that owns the transaction.

## API Design

### `POST /transactions`

- Validates all fields before enqueueing.
- Returns `202 Accepted` after Redis confirms `XADD`.
- Returns a stable response containing the event `id` and status `accepted`.
- Returns `422` for invalid input.
- Returns `503 Service Unavailable` if Redis cannot accept the event. A request is
  never reported as accepted unless Redis confirms the append.
- Accepting the same `id` more than once is allowed because final idempotency is
  enforced by the consumer and database.

### `GET /users/{user_id}/summary`

- Returns `200 OK` with `user_id`, `total_usd`, and `transaction_count`.
- Serializes `total_usd` as a decimal string so JSON conversion does not
  introduce binary floating-point error.
- Computes `SUM(amount_usd)` and `COUNT(*)` from persisted transactions.
- Returns zero values when the user has no transactions.
- A separate summary table is deliberately avoided at this load because it adds
  transaction and consistency complexity.

### `GET /users/{user_id}/transactions`

- Optional `from` and `to` timestamps are inclusive and normalized to UTC.
- Uses offset pagination because it is simpler to implement and explain for this
  assessment's expected data size.
- Default `limit` is 50; maximum is 200.
- Results are ordered by `(event_timestamp DESC, id DESC)`.
- Default `offset` is 0.
- Returns `200 OK` with `items`, `limit`, `offset`, and `has_more`.
- Each item contains `id`, `user_id`, `original_amount`,
  `original_currency`, `usd_rate`, `amount_usd`, `event_timestamp`, and
  `processed_at`. Money and rate fields are decimal strings.
- Returns `422` for malformed query parameters, including negative pagination
  values or `from > to`.
- Returns explicit response schemas, never ORM objects.

### `GET /metrics`

- Exposes Prometheus text format.
- Exposes the required basic metric as a PostgreSQL-backed
  `transaction_records` gauge.
- The API executes `COUNT(*)` against the source-of-truth table when metrics are
  scraped. This is exact, works across separate API and worker containers, and
  avoids adding a second persisted counter for the take-home.

All database-backed GET endpoints return `503 Service Unavailable` with a stable
`{"detail": {"code": "...", "message": "..."}}` error shape when PostgreSQL is
unavailable. Empty users return successful zero/empty responses rather than
`404`, because users are not modeled as separate resources.

## Data Flow

1. FastAPI validates the request using a Pydantic schema.
2. The ingestion service serializes a versioned event payload and appends it to
   the Redis stream using `XADD`.
3. The API returns `202` only after Redis confirms the stream entry ID.
4. A worker reads new entries using `XREADGROUP` and a consumer group.
5. The worker validates the queued payload again because queue data is an
   infrastructure trust boundary.
6. The worker resolves a USD conversion rate and calculates the USD amount using
   `Decimal` and the documented rounding rule.
7. Within one SQLAlchemy async transaction, the worker executes PostgreSQL
   `INSERT ... ON CONFLICT (id) DO NOTHING RETURNING id`.
8. A returned ID means a new row was stored. No returned ID means the event was
   already stored and is an idempotent success.
9. The transaction context exits successfully and commits.
10. Only after the database transaction has committed, the worker acknowledges
    the Redis entry with `XACK`. A duplicate is also acknowledged only after its
    conflict-safe database transaction completes successfully.
11. Read APIs and the basic metric query PostgreSQL and do not depend on the
    rate provider.

If the process crashes after the database commit but before `XACK`, the message
is delivered again. The unique constraint turns that second processing attempt
into a no-op, after which it is acknowledged.

## Event Contract

The Redis entry will contain a small versioned JSON payload:

```json
{
  "version": 1,
  "id": "transaction UUID",
  "user_id": "user UUID",
  "amount": "12.34",
  "currency": "EUR",
  "timestamp": "2026-06-06T12:00:00Z"
}
```

Money is serialized as a string to preserve decimal precision. Versioning allows
future additive changes and gives the consumer an explicit response to
unsupported payload versions.

## Database Schema

PostgreSQL will contain one application table:

### `transactions`

| Column | Type | Constraints / purpose |
|---|---|---|
| `id` | UUID | Primary key; source event ID and deduplication key |
| `user_id` | UUID | Not null |
| `original_amount` | NUMERIC(20, 8) | Not null, greater than zero |
| `original_currency` | VARCHAR(3) | Not null; exactly three uppercase characters |
| `usd_rate` | NUMERIC(20, 10) | Not null, greater than zero |
| `amount_usd` | NUMERIC(20, 2) | Not null, greater than or equal to zero |
| `event_timestamp` | TIMESTAMPTZ | Not null; timestamp supplied by producer |
| `processed_at` | TIMESTAMPTZ | Not null; server-side processing time |

Indexes:

- Primary key on `id` for durable deduplication.
- Composite index on
  `(user_id, event_timestamp DESC, id DESC)` for filtered and consistently
  ordered transaction queries.

Database check constraints enforce positive original amounts and rates,
non-negative converted amounts, and the currency storage format. API validation
will still provide friendlier errors before persistence.

The same composite index supports the per-user summary adequately at the target
volume. A summary table or materialized view will only be introduced if measured
query cost justifies it.

`amount_usd` is rounded to two decimal places using `ROUND_HALF_UP`. The original
amount and applied rate are retained for auditability and to make conversion
behavior explainable.

Alembic will own schema creation. The initial migration is additive. Downgrade
can drop the table because this is a new local assessment service with no
pre-existing production data; in a real production system, a forward fix would
usually be safer than dropping financial records.

## SQLAlchemy 2.0 Async Usage

- Use SQLAlchemy 2.x typed declarative mappings.
- Use PostgreSQL through an async driver.
- Create one `AsyncSession` per HTTP request or worker processing attempt.
- Use `async with session.begin()` so transaction ownership is explicit.
- Use `select()` and SQLAlchemy 2.x execution APIs.
- Do not return ORM models from HTTP routes.
- Do not use implicit lazy loading.
- Do not call `commit()` from helper query functions; the use case owns commit.
- Handle uniqueness through the database constraint, not a check-then-insert
  query that would race under concurrent consumers.

## Queue Design

### Choice

Use Redis Streams with:

- Stream: `transactions`
- Consumer group: `transaction-processors`
- One unique consumer name per worker process
- Dead-letter stream: `transactions:dead-letter`

### Why Redis Streams

- Consumer groups distribute work across worker instances.
- The pending entries list tracks delivered but unacknowledged messages.
- Explicit acknowledgements support at-least-once processing.
- Redis is simple to run in Docker Compose and appropriate for the stated load.
- `XADD`, `XREADGROUP`, `XAUTOCLAIM`, and `XACK` directly
  expose the delivery mechanics needed for the interview.

### Alternatives Considered

- **RabbitMQ:** stronger queue-oriented routing and dead-letter features, but adds
  another operational model and more configuration than this service needs.
- **Kafka:** excellent durability and partitioned throughput, but excessive for
  100 events/second and a small locally run assessment.
- **PostgreSQL queue table:** reduces infrastructure, but requires polling and
  row-locking logic and does not exercise the requested Redis Streams concepts.
- **Redis lists:** simpler enqueue/dequeue operations, but consumer groups and
  pending-message inspection are absent.

### Accepted Trade-Off

Redis Streams are less durable than Kafka or a database-backed queue under a
catastrophic Redis data loss. Docker Compose will enable Redis AOF persistence
and use a named volume, but this is still not a cross-region durable log.

The stream will have no aggressive automatic trimming in the initial version.
Acknowledged entries may be trimmed by a maintenance policy later, only after a
retention window. Trimming entries still present in the pending list must be
avoided.

## Consumer Groups and Acknowledgements

- The worker creates the consumer group idempotently on startup using
  `XGROUP CREATE transactions transaction-processors 0-0 MKSTREAM`. Starting at
  `0-0`, rather than `$`, ensures events appended before worker startup are not
  skipped.
- New messages are read with `XREADGROUP ... >`.
- Each message is processed independently so one failure does not roll back a
  batch of unrelated messages.
- `XACK` occurs only after a successful database transaction has committed. For
  duplicates, that transaction contains the conflict-safe insert that confirms
  the existing ID.
- `XDEL` is not required for correctness; acknowledgement and retention are
  separate concerns.
- A graceful shutdown stops reading new messages, finishes the active attempt,
  and leaves unfinished messages pending for reclamation.

## Idempotency and Deduplication

The database primary key on `transactions.id` is the final idempotency mechanism.
This is chosen over an in-memory or Redis-only deduplication key because:

- It is atomic with persistence.
- It survives application and Redis restarts.
- It prevents races between multiple consumers.
- It cannot report an event as processed without the stored transaction existing.

The worker uses one specific race-safe operation:
`INSERT ... ON CONFLICT (id) DO NOTHING RETURNING id`. It does not perform a
check-then-insert query and does not use exception-driven rollback for the normal
duplicate path.

The first successfully stored payload wins. If a later event reuses the same ID
with different fields, it is still treated as a duplicate and does not overwrite
the financial record. Comparing every duplicate payload with the stored row is
deliberately omitted to keep the hot path and implementation small.

## Currency Conversion

- Conversion is a pure function once `amount` and `rate` are available.
- `Decimal` is used throughout.
- `amount_usd = (amount * rate).quantize(Decimal("0.01"), ROUND_HALF_UP)`.
- Unsupported currencies are non-retryable because repeating the same payload
  cannot fix it.
- A temporary provider timeout or unavailable provider is retryable.
- The local configured provider is deterministic and keeps tests and Docker
  startup independent of the internet.
- The provider boundary is intentionally narrow: fetch one USD rate for one
  currency. No generic integration framework is planned.

## Retry Strategy

Failed messages are not acknowledged, so they remain in the consumer group's
pending entries list.

For a retryable database or rate-provider failure, the worker keeps ownership of
the pending entry and retries the same processing function:

1. Classify the exception as retryable.
2. Compute exponential backoff with bounded jitter:
   `min(base * 2^(attempt - 1), maximum) + jitter`.
3. Use `XCLAIM` for the same consumer and entry before each sleep/attempt to
   refresh its idle time, so a live retry is not eligible for orphan reclamation.
4. Sleep without acknowledging the entry.
5. Retry until processing succeeds or the process stops.
6. On success, commit the database transaction and then `XACK`.

Initial values:

- Base delay: 1 second
- Maximum delay: 60 seconds
- Orphan reclaim timeout: 120 seconds

Transient failures are retried indefinitely with a capped delay. This directly
satisfies "do not lose events" and avoids persistent retry metadata, a scheduler,
and an arbitrary point at which an unavailable downstream causes valid financial
events to be abandoned.

A small reclaim loop periodically calls `XAUTOCLAIM` with the orphan reclaim
timeout. It handles pending entries whose worker crashed or was terminated.
Active retry loops refresh their pending-entry ownership, so normal backoff does
not look like abandonment. A reclaimed message runs through the same idempotent
processing path. Multiple workers may still race during failures, so the
database primary key remains the final correctness mechanism.

Non-retryable payload failures, such as an unsupported event version or currency,
are copied to the dead-letter stream before the original is acknowledged. The
copy and acknowledgement use a short Redis transaction (`MULTI`/`EXEC`) so a
crash cannot acknowledge the original without retaining the failed payload.
Database connectivity errors, rate-provider timeouts, and other explicitly
classified transient failures use indefinite capped backoff. Unexpected
exceptions are retried five times and then copied to the DLQ, because an
unclassified code or payload defect must not block a worker forever. The payload
is retained, so this is isolation rather than loss.

This deliberately favors correctness and simplicity over fairness: while a
single worker waits for a downstream outage to recover, its throughput is
reduced. Additional worker replicas can continue processing, and the stated load
does not justify a separate delayed-retry scheduler.

## Delivery Guarantee

The system provides **at-least-once delivery with idempotent processing**, not
exactly-once delivery.

Exactly-once delivery across Redis and PostgreSQL would require a distributed
transaction or a different architecture with a shared transactional boundary.
Neither is justified here. The acknowledged trade-off is that a message can be
processed more than once after a crash, while the database uniqueness constraint
ensures only one transaction row is stored.

There is also a producer-side ambiguity if Redis accepts `XADD` but the HTTP
connection fails before the response reaches the client. A client may retry;
the event ID makes this safe.

## Failure Scenarios

| Scenario | Behavior |
|---|---|
| Redis unavailable during ingestion | Return `503`; do not claim acceptance |
| API crashes after `XADD` before response | Client may retry; DB deduplication makes duplicate delivery safe |
| Worker crashes before DB commit | Message remains pending and is reclaimed |
| Worker crashes after DB commit before `XACK` | Message is redelivered; unique ID makes processing a no-op |
| PostgreSQL unavailable | Leave pending and retry with capped backoff |
| Rate provider temporarily unavailable | Leave pending and retry with backoff |
| Unsupported currency/version | Copy to DLQ and acknowledge original in one Redis transaction |
| Unexpected processing exception | Retry five times, then retain in DLQ |
| Redis restarts | AOF and named volume recover accepted stream data within Redis persistence guarantees |
| Retry worker crashes | Due pending entries can be reclaimed by another worker |
| Duplicate IDs arrive concurrently | Database primary key selects one winner; the other is acknowledged as duplicate |
| Duplicate ID has different payload | Preserve first record and do not overwrite |
| API process restarts | Metric remains exact because it is derived from PostgreSQL |

## Observability

The required metric is:

- `transaction_records`: a Prometheus gauge set from `SELECT COUNT(*) FROM
  transactions`.

PostgreSQL is already the source of truth, so this metric is exact across process
restarts and duplicate deliveries. Additional counters and histograms are
deferred to keep the take-home implementation small.

Structured logs will include:

- Event ID
- Redis stream entry ID
- Consumer name
- Attempt number
- Outcome and stable error category
- Processing duration

Logs must not include stack traces in API responses, database credentials, or
unnecessary full payloads. Health checks should distinguish process liveness
from dependency readiness.

## Testing Strategy

### Unit Tests

- Currency conversion:
  - USD rate of 1
  - non-USD multiplication
  - `ROUND_HALF_UP` boundary behavior
  - large decimal precision
  - unsupported currency
  - invalid/non-positive amounts
- Deduplication service behavior:
  - first event is stored
  - repeated ID does not create a second row
  - duplicate is considered successfully handled
  - differing payload with same ID does not overwrite the first row
- Retry classification and backoff calculation with deterministic jitter.
- Acknowledgement orchestration:
  - successful commit is followed by `XACK`
  - failed commit is never followed by `XACK`
  - duplicate conflict-safe transaction is followed by `XACK`

### Integration Tests

- PostgreSQL unique constraint under duplicate and concurrent inserts.
- SQLAlchemy async transaction rollback and commit behavior.
- Redis consumer-group flow: read, pending state, reclaim, and acknowledge.
- Worker crash-equivalent case: persisted row plus unacknowledged message is
  safely reprocessed.
- Retryable dependency failure leaves a message pending.
- Reclaimed pending message is processed safely.
- Non-retryable event reaches the DLQ before the original is acknowledged.
- Unexpected failures reach the DLQ after five attempts.
- API validation, `202`, `503`, summary aggregation, filters, ordering, and offset
  pagination.
- `/metrics` returns the PostgreSQL-backed record count.

Unit tests will mock only the rate-provider boundary and infrastructure failures.
Database and Redis behavior should be covered with real disposable services
because mocks cannot validate uniqueness, transactions, or pending-entry
semantics.

## Docker Strategy

Docker Compose will define:

- `api`: FastAPI served by an ASGI server.
- `worker`: the same application image with a worker command.
- `postgres`: PostgreSQL with a health check and named volume.
- `redis`: Redis with AOF enabled, health check, and named volume.

The API and worker use the same image to avoid drift. Configuration is supplied
through environment variables with non-secret local defaults. No credentials
are embedded in the image.

Startup behavior:

- Compose health checks establish dependency readiness.
- Alembic migrations run through an explicit one-shot migration command/service
  before API and worker startup, avoiding multiple application instances racing
  to migrate.
- The worker creates the Redis consumer group idempotently.
- Services retry dependency connection during startup with bounded delays rather
  than relying only on Compose startup order.

The image will use Python 3.12, install dependencies from the `uv` lock file, run
as a non-root user, and use an exec-form command so shutdown signals reach the
application.

## Python Tooling

- Python 3.12.
- `uv` for dependency management and the lock file.
- `pyproject.toml` as the single project configuration source.
- Ruff for formatting, linting, and import ordering.
- mypy for practical static checking of `app`.
- pytest and pytest-asyncio for tests.

No Poetry, pip-tools, Black, Flake8, or separate isort configuration will be
introduced.

## README Plan

`README.md` will include:

- A short architecture overview and request-to-worker data flow.
- Prerequisites and local startup with `docker compose up --build`.
- Example requests for ingestion, summary, paginated transactions, and metrics.
- Why Redis Streams was chosen.
- The at-least-once delivery and idempotency explanation.
- One explicit accepted trade-off: Redis durability is weaker than a durable
  replicated log, accepted for local simplicity.
- What would change at 10x load.
- How database and rate-provider failures are retried.
- Required local commands:

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy app
docker compose up --build
```

- The public GitHub or GitLab repository URL before submission.

## Important Technical Decisions

### Redis Streams over a dedicated broker

- **Chosen:** Redis Streams and consumer groups.
- **Why:** sufficient throughput, simple local operation, explicit pending and
  acknowledgement semantics.
- **Alternatives:** RabbitMQ, Kafka, PostgreSQL queue.
- **Trade-off:** weaker catastrophic-loss guarantees and more manual retry/DLQ
  handling than specialized brokers.

### At-least-once over exactly-once

- **Chosen:** acknowledge after commit and make processing idempotent.
- **Why:** Redis and PostgreSQL do not share a transaction.
- **Alternative:** distributed transactions or a single database-backed queue.
- **Trade-off:** duplicate execution is possible, but duplicate stored records
  are prevented.

### Database constraint over pre-check deduplication

- **Chosen:** primary key on source event ID.
- **Why:** atomic, durable, and race-safe.
- **Alternative:** query before insert or Redis deduplication keys.
- **Trade-off:** duplicate attempts reach PostgreSQL and must handle conflicts.

### In-place retry over a delayed-retry scheduler

- **Chosen:** leave transient failures pending and retry in the worker with
  capped exponential backoff; reclaim only work abandoned by crashed consumers.
- **Why:** directly preserves accepted events with few moving parts.
- **Alternatives:** retry metadata plus a scheduler, a retry stream, or broker
  dead-letter delays.
- **Trade-off:** a waiting worker has lower throughput during an outage.

### Query-time summary over a summary table

- **Chosen:** `SUM` and `COUNT` on stored transactions.
- **Why:** simplest correct design at the expected load.
- **Alternatives:** transactional summary row, materialized view, analytics store.
- **Trade-off:** summary latency grows with each user's transaction count.

### Offset over keyset pagination

- **Chosen:** `LIMIT/OFFSET` with deterministic timestamp and ID ordering.
- **Why:** smallest API and implementation for an assessment-sized data set.
- **Alternative:** keyset cursor pagination.
- **Trade-off:** large offsets become slower and can shift while new rows arrive.

### Configured local rates behind a provider boundary

- **Chosen:** deterministic offline local provider with an injectable interface.
- **Why:** Compose works without an API key or external network and conversion is
  easy to test.
- **Alternatives:** live public rate API or a separate mock-rate container.
- **Trade-off:** local rates are not real-time; a production deployment would
  replace the provider and define rate freshness/historical semantics.

### Database-backed gauge over process-local counters

- **Chosen:** expose the current stored transaction count from PostgreSQL.
- **Why:** exact across API and worker processes and trivial to explain.
- **Alternatives:** Prometheus multiprocess mode, a Redis counter, or a separate
  worker metrics server.
- **Trade-off:** each scrape performs a simple `COUNT(*)` query.

## What Would Change at 10x Load

At sustained 1,000 events/second, with larger bursts:

- Run multiple worker replicas with distinct consumer names and tune batch size
  and database connection pools based on measurements.
- Partition work if one Redis stream or consumer group becomes a bottleneck,
  likely by a stable hash of `user_id`.
- Cache remote currency rates with explicit freshness and stale-rate policy to
  avoid one lookup per event.
- Use PostgreSQL bulk inserts where compatible with per-message error handling.
- Consider a transactional per-user summary table if aggregate-query latency is
  measured to be unacceptable. Updates would occur in the same transaction as
  the insert and only when the insert is new.
- Add stream retention based on age and acknowledged state, plus automated DLQ
  replay tooling.
- Export metrics to Prometheus/Grafana and alert on pending count, oldest pending
  age, retry rate, DLQ growth, and database pool saturation.
- Inspect query plans and consider table partitioning by event time only after
  table size demonstrates a need.
- Reassess Redis durability. If event loss tolerance approaches zero or replay
  volume becomes important, move to a replicated durable broker such as Kafka.
- Add load tests that cover burst ingestion, consumer recovery, and downstream
  outage recovery rather than optimizing from assumptions.

## Approval Gate

Implementation should begin only after this architecture is reviewed and
approved. The first implementation slice should establish Docker Compose,
configuration, migrations, and the minimal ingest-to-worker-to-database path
before adding read APIs, retries, metrics, and broader tests.
