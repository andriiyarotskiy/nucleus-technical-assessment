# Transaction Event Service

Async transaction processing service built with FastAPI, Redis Streams,
PostgreSQL, and SQLAlchemy. Detailed design and failure handling are documented
in [docs/architecture.md](docs/architecture.md).

## Run

Requires Docker with Docker Compose:

```bash
docker compose up --build
```

The committed `.env.example` contains local-only defaults used by Docker
Compose, so no setup step is required. The API is available at
`http://localhost:8000`, with interactive docs at `http://localhost:8000/docs`.

## Verification

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/):

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy app
docker compose up --build
```

## Why Redis Streams

Redis Streams provides consumer groups, acknowledgements, and pending-message
recovery while keeping the local stack small. It supports the required
at-least-once delivery model without introducing a separate message broker.

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
