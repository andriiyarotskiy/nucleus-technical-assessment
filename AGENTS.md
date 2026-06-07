# Project Agent Guide

This repository is a small backend technical assessment. Follow `TASK.md` as the
source of truth.

The solution must be easy to explain and modify during a live interview. Prefer
less code and fewer moving parts when they still satisfy the requirements.

## Before Implementation

1. Read `TASK.md`, the existing code, tests, and configuration.
2. Create or update `docs/architecture.md` with the required implementation
   plan.
3. For important decisions, explain the choice, reason, alternatives, and
   accepted trade-off.
4. Wait for approval before starting a new implementation or changing the agreed
   architecture.

For a small fix within the approved architecture, state a short plan and
proceed.

## Implementation Principles

Prioritize:

1. Simplicity.
2. Correctness.
3. Maintainability.
4. Explicit error handling.
5. Testability.

Rules:

- Make the smallest change that fully satisfies the task.
- Prefer direct functions and explicit control flow.
- Keep modules and classes focused and few.
- Keep logic close to where it is used.
- Add an abstraction only when it removes real duplication or is required to
  replace or test an external dependency.
- Do not add generic repositories, base services, factories, framework wrappers,
  or speculative extension points.
- Do not optimize for requirements beyond the stated load.
- Do not refactor unrelated code or overwrite user changes.
- Every non-obvious design choice must be explainable in plain language.

## Required Stack

Use only the stack required by the task and already present in the project:

- Python 3.12
- FastAPI
- Redis Streams and consumer groups
- PostgreSQL
- SQLAlchemy 2.x async
- Alembic
- Docker Compose
- `uv` and `pyproject.toml`
- Ruff
- mypy where practical
- pytest and pytest-asyncio

Do not add dependencies unless the task cannot be completed clearly without
them.

## Implementation Requirements

- Accept and validate transaction events through FastAPI.
- Append accepted events to Redis Streams.
- Process events asynchronously with a consumer group.
- Deduplicate by event `id` using a database constraint.
- Convert money with `Decimal` and an explicit rounding rule.
- Use timezone-aware UTC datetimes.
- Use at-least-once delivery.
- Acknowledge a message only after successful database persistence.
- Preserve unacknowledged messages during temporary database or rate lookup
  failures and retry them with backoff.
- Handle pending messages so events abandoned by a worker can be processed
  again.
- Keep SQLAlchemy session and transaction boundaries explicit.
- Expose the required read APIs and one basic metric.
- Return clear errors without exposing raw infrastructure exceptions.

Keep this behavior explicit in the code. Do not hide Redis or SQLAlchemy calls
behind generic infrastructure layers.

## Testing

- Add focused tests for deduplication and currency conversion.
- Test important failure behavior such as retries and acknowledgement timing
  where practical.
- Prefer readable behavior tests over tests of internal implementation details.
- Mock only external boundaries.
- Do not weaken tests to make code pass.

## Verification

Run the relevant project commands:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy app
docker compose config
```

Report commands that were not run and why.

## Documentation

Keep `docs/architecture.md` and `README.md` consistent with the implementation.
Document:

- queue choice;
- retry and pending-message behavior;
- acknowledgements and at-least-once delivery;
- idempotency and deduplication;
- one accepted trade-off;
- what would change at 10x load;
- local setup and verification commands.

## Definition Of Done

- All requirements from `TASK.md` are implemented.
- The code is short, direct, and explainable line by line.
- Failure handling does not lose accepted events.
- Relevant tests and checks pass.
- No unnecessary abstractions, dependencies, or unrelated changes were added.
