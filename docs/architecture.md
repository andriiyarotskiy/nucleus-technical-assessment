# Transaction Event Service Architecture

## 1. Executive Summary

The service accepts transaction events, queues them in Redis Streams, converts
amounts to USD, and stores idempotent transaction records in PostgreSQL.

| Area | Decision |
|---|---|
| API | FastAPI with explicit Pydantic request and response schemas |
| Queue | Redis Streams with one consumer group |
| Persistence | PostgreSQL through SQLAlchemy 2.x async |
| Delivery | At-least-once |
| Deduplication | PostgreSQL primary key on event `id` |
| Money | `Decimal`, rounded with `ROUND_HALF_UP` |
| Retry | Unacknowledged pending entries reclaimed with `XAUTOCLAIM` |
| Deployment | API, worker, PostgreSQL, Redis, and migrations in Docker Compose |

The design targets approximately 100 events/second with short bursts near 1,000.
It favors direct, interview-explainable control flow over framework abstractions.

## 2. Architecture Diagram

```text
                         GET summary / transactions
                    +--------------------------------+
                    |                                |
Client --> FastAPI API --> Redis Stream --> Worker --> PostgreSQL
           POST /events      XADD          |          transactions
                                          |
                                          +--> fixed USD rate provider
                                          |
                                          +--> Redis metrics hash

Redis consumer group:
  new entries     -> XREADGROUP
  failed entries  -> pending entries list
  recovery        -> XAUTOCLAIM
  success         -> XACK after DB commit
  invalid events  -> dead-letter stream, then XACK
```

Package responsibilities:

| Package | Responsibility |
|---|---|
| `app/api` | FastAPI routes and dependency wiring |
| `app/schemas` | HTTP and queued-event contracts |
| `app/services` | Conversion, event publishing, metrics, processing decisions |
| `app/db` | SQLAlchemy models, sessions, repositories, migrations |
| `app/worker` | Redis consumer loop, recovery, ACK and DLQ orchestration |
| `app/core` | Configuration and logging |

## 3. Key Decisions

| Decision | Rationale | Accepted trade-off |
|---|---|---|
| Redis Streams | Consumer groups, pending entries, explicit ACKs, simple Compose setup | Less durable than Kafka or a database-backed queue under catastrophic Redis loss |
| At-least-once delivery | Redis and PostgreSQL do not share a transaction | Processing may repeat; stored rows do not |
| DB primary key deduplication | Atomic, durable, and race-safe | Duplicate attempts still reach PostgreSQL |
| Pre-check plus conflict-safe insert | Avoid rate lookup for known duplicates while preserving concurrency safety | Two DB operations for a new event |
| Query-time summary | Correct and simple at the expected scale | Aggregate latency grows with user history |
| Offset pagination | Easy API and implementation | Large offsets become slower and may shift as rows are added |
| Fixed in-memory rates | Deterministic, offline, no API key | Rates are not current or historical |
| Redis-backed counters | Shared by separate API and worker processes | Best-effort metrics depend on Redis |
| Focused repository | Keeps SQLAlchemy and transaction boundaries in `db` | One concrete repository, not a generic persistence framework |

## 4. Data Flow

### Ingestion

1. `POST /events` validates UUIDs, positive decimal amount, uppercase currency,
   and timezone-aware timestamp.
2. The producer writes a versioned JSON payload with `XADD`.
3. The API returns `202 Accepted` only after Redis confirms the append.
4. Redis failures return `503`; PostgreSQL is not touched by this endpoint.

### Processing

1. The worker reads new entries through `XREADGROUP`.
2. The queued payload is validated again at the queue boundary.
3. The processor checks whether the transaction ID already exists.
4. New events are converted using `Decimal`.
5. The repository executes:

   ```sql
   INSERT ... ON CONFLICT (id) DO NOTHING RETURNING id
   ```

6. Exiting `async with session.begin()` commits the transaction.
7. The worker sends `XACK` only after the repository returns successfully.

### Reads

- `GET /users/{user_id}/summary` calculates `SUM(amount_usd)` and `COUNT(*)`.
- `GET /users/{user_id}/transactions` applies optional inclusive UTC bounds,
  offset pagination, and deterministic descending order.
- `GET /metrics` renders Redis-backed counters in Prometheus text format.

## 5. Database Design

One table is sufficient for the assignment:

| Column | Type | Purpose |
|---|---|---|
| `id` | UUID primary key | Source event ID and deduplication key |
| `user_id` | UUID, not null | Query partition |
| `original_amount` | NUMERIC(20, 8) | Original decimal value |
| `original_currency` | VARCHAR(3) | Uppercase source currency |
| `usd_rate` | NUMERIC(20, 10) | Applied conversion rate |
| `amount_usd` | NUMERIC(20, 2) | Rounded USD result |
| `event_timestamp` | TIMESTAMPTZ | Producer timestamp |
| `processed_at` | TIMESTAMPTZ | Database processing timestamp |

Constraints enforce positive amounts and rates, non-negative USD values, and
currency format.

Indexes:

- Primary key on `id` for durable idempotency.
- `(user_id, event_timestamp DESC, id DESC)` for filtered transaction lists and
  per-user scans.

SQLAlchemy rules:

- Async engine and one short-lived `AsyncSession` per repository operation.
- Explicit `session.begin()` owns the write transaction.
- No implicit lazy loading or ORM serialization from routes.
- Alembic owns schema changes.

## 6. Queue & Delivery Guarantees

Redis configuration:

| Item | Value |
|---|---|
| Stream | `transactions` |
| Consumer group | `transaction-processors` |
| Dead-letter stream | `transactions:dead-letter` |
| Persistence | AOF with a named Docker volume |

Guarantee: **at-least-once delivery with idempotent database persistence**.

- The group starts at `0-0`, so events created before worker startup are read.
- New work uses `XREADGROUP ... >`.
- Successful inserts are acknowledged after commit.
- Confirmed duplicates are acknowledged as successful no-ops.
- A crash after commit but before `XACK` causes redelivery; the primary key
  prevents a second row.
- Producer ambiguity after a successful `XADD` is safe because clients can retry
  with the same event ID.

Exactly-once delivery is intentionally not claimed. It would require a shared
transactional boundary or distributed transaction across Redis and PostgreSQL.

## 7. Retry Strategy

- Retryable failures are **not acknowledged**.
- Failed entries remain in the consumer group's pending entries list.
- Before reading new entries, the worker calls `XAUTOCLAIM`.
- Entries idle for five seconds are processed through the same idempotent path.
- The claim cursor is retained so recovery can scan the pending list.
- Redis connection failures pause the worker for the same configured delay.

The current policy is a fixed delay with unlimited attempts. This avoids a retry
table, scheduler, and attempt metadata in a small assessment.

Terminal failures:

- Invalid payload or unsupported currency is written to the DLQ.
- The original entry is acknowledged only after the DLQ append succeeds.
- A crash between DLQ append and `XACK` may duplicate the DLQ entry.
- DLQ consumers should use `source_id` as an idempotency key.

## 8. Failure Scenarios

| Scenario | Result |
|---|---|
| Redis unavailable during `POST /events` | Return `503`; do not report acceptance |
| API crashes after `XADD` | Client may retry; DB deduplication is safe |
| Worker crashes before commit | Entry remains pending and is reclaimed |
| Worker crashes after commit, before `XACK` | Redelivery becomes a duplicate no-op |
| PostgreSQL unavailable | No ACK; retry from pending list |
| Temporary rate lookup failure | No ACK; retry from pending list |
| Unsupported currency or invalid payload | Append to DLQ, then ACK original |
| DLQ append fails | Original remains pending |
| Concurrent duplicate IDs | Primary key selects one stored row |
| Duplicate ID with different data | First stored record wins; no overwrite |
| Metrics update fails | Transaction result remains valid; metric may undercount |
| Redis data loss | Recovery is limited to Redis AOF and volume guarantees |

## 9. Testing Strategy

Current assignment-focused unit tests cover:

- `100 USD -> 100.00 USD`.
- `100 EUR -> 108.00 USD`.
- Unsupported currency error.
- Retryable rate-provider failure propagation.
- Existing event ID skips conversion and insert.
- Database conflict determines the final duplicate outcome.

Verification commands:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy app
docker compose config
```

Deliberate gap: real Redis pending-entry recovery and PostgreSQL concurrency are
not integration-tested in this take-home. Production confidence would require
disposable Redis/PostgreSQL integration tests for ACK timing, rollback,
`XAUTOCLAIM`, and concurrent inserts.

## 10. Trade-Offs

### Simplicity accepted

- Fixed rates instead of an external rate API.
- Fixed-delay, unlimited retries instead of exponential backoff and max attempts.
- Offset pagination instead of cursor pagination.
- Query-time summaries instead of a maintained summary table.
- Best-effort Redis counters instead of a dedicated metrics stack.
- Non-atomic DLQ append plus ACK instead of a Redis transaction or Lua script.

### Intentionally excluded

- Authentication and authorization.
- Historical exchange-rate semantics.
- Automated DLQ replay.
- Aggressive stream trimming.
- Generic repositories, base services, or dependency-injection frameworks.

These choices keep the implementation small enough to explain and modify during
an interview while preserving the core reliability requirements.

## 11. 10x Scaling Plan

If sustained throughput grows toward 1,000 events/second:

1. Add worker replicas with unique consumer names.
2. Tune Redis batch size and PostgreSQL connection pools using load tests.
3. Cache external exchange rates with explicit freshness rules.
4. Replace offset pagination with keyset pagination for deep lists.
5. Add a transactional per-user summary table only if query measurements justify
   it.
6. Partition streams by stable `user_id` hash if one stream becomes a bottleneck.
7. Add retention policies, DLQ replay tools, and alerts for pending age and DLQ
   growth.
8. Add real Redis/PostgreSQL integration and outage-recovery tests.
9. Reassess Redis durability; move to a replicated durable broker if event-loss
   tolerance or replay requirements demand it.
