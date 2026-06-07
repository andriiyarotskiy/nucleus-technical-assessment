# Transaction Event Service

Minimal Python scaffolding for the asynchronous transaction processing service
described in [docs/architecture.md](docs/architecture.md). Business endpoints,
queue consumption, persistence, and currency conversion are not implemented yet.

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
