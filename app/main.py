from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from app.api.routes import router
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.repositories.transactions import TransactionRepository
from app.db.session import build_async_engine, build_session_factory
from app.services.events import RedisStreamEventProducer
from app.services.metrics import RedisMetricsStore


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    database_engine = build_async_engine(settings.postgres_dsn)
    application.state.event_producer = RedisStreamEventProducer(
        redis,
        settings.redis_stream_name,
    )
    application.state.metrics_store = RedisMetricsStore(
        redis,
        settings.redis_metrics_key,
    )
    application.state.transaction_repository = TransactionRepository(
        build_session_factory(database_engine)
    )
    yield
    await redis.aclose()
    await database_engine.dispose()


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings)
    application = FastAPI(
        title=settings.app_name,
        summary="Asynchronous transaction event processing API",
        description=(
            "Accepts transaction events, processes them asynchronously through "
            "Redis Streams, and exposes persisted user transaction views. "
            "Processing is at-least-once and database writes are idempotent by "
            "event ID."
        ),
        version="1.0.0",
        openapi_tags=[
            {
                "name": "Events",
                "description": "Accept transaction events for asynchronous processing.",
            },
            {
                "name": "Users",
                "description": "Read persisted transaction data and aggregates.",
            },
            {
                "name": "Observability",
                "description": "Inspect basic worker processing counters.",
            },
        ],
        lifespan=lifespan,
    )
    application.include_router(router)
    return application


app = create_app()
