## Original Assignment
Build a small async event processing service:

1. HTTP endpoint to receive transaction events: `{id, user_id, amount, currency, timestamp}`.
2. Push events to a queue (your choice).
3. Consumer reads from queue, deduplicates by `id`, converts amount to USD, stores result.
4. Handle a failing downstream: if the DB or rate lookup is unavailable, do not lose events. Retry with backoff. Document your choice (at-least-once vs exactly-once).
5. APIs:
   - `GET /users/{user_id}/summary` → total USD + transaction count.
   - `GET /users/{user_id}/transactions?from=&to=` → paginated list.
6. One basic metric exposed (e.g. events processed, lag, failures).

## Rules
- Language should be python.
- Keep it simple. Assume ~100 events/sec, bursts to ~1k/sec.
- Docker Compose to run everything locally.
- README: how to run, why you picked your queue, one trade-off you made, what you'd change at 10x load.
- Unit tests for dedup + currency conversion.
- Push the solution to a public git repo (GitHub, GitLab, etc.) and share the link.

## Follow-Up Interview
You will walk us through your source code. We will pick random lines and ask you to explain them. **You must show you understand every part of what you submitted.** Be ready to modify a piece live.

---

# Implementation Instructions

Before writing code, create an implementation plan.

Act as a senior backend engineer.

Prioritize:
- simplicity
- correctness
- maintainability
- explicit error handling
- testability

Avoid:
- overengineering
- unnecessary abstractions
- generic repository patterns
- premature optimization

Every design choice must be explainable in a technical interview.

The solution should be simple enough that I can explain and modify it live during the follow-up interview.

For every important technical decision, explain:
- what was chosen
- why it was chosen
- what alternatives were considered
- what trade-off was accepted

Pay special attention to:
- Redis Streams
- consumer groups
- acknowledgements
- pending messages
- retry with backoff
- idempotency and deduplication
- at-least-once delivery
- SQLAlchemy 2.0 async usage
- FastAPI API design
- Docker Compose
- metrics
- unit tests
- README explanation

Do not start implementation immediately.

First create:

docs/architecture.md

The architecture document should include:
- requirements summary
- assumptions
- high-level architecture
- data flow
- database schema
- queue design
- retry strategy
- delivery guarantee
- failure scenarios
- observability
- testing strategy
- Docker strategy
- trade-offs
- what would change at 10x load

Wait for approval before writing implementation code.

# Python Tooling

Use:

- Python 3.12
- uv for dependency management
- pyproject.toml as the single source of project configuration
- ruff for formatting and linting
- mypy for static type checking where practical
- pytest for tests
- pytest-asyncio for async tests

Do not use:

- Poetry
- pip-tools
- Black as a separate formatter
- Flake8
- isort as a separate tool

Reason:

ruff covers formatting, linting, and import sorting with less configuration.
uv keeps dependency management simple and fast.
The tooling should stay easy to explain during the interview.

Required commands should be documented in README:

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy app
docker compose up --build