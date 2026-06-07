from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from app.api import router
from app.config import get_settings
from app.db.session import build_async_engine, build_session_factory
from app.events import RedisStreamEventProducer
from app.logging import configure_logging
from app.metrics import RedisMetricsStore


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
    application.state.session_factory = build_session_factory(database_engine)
    yield
    await redis.aclose()
    await database_engine.dispose()


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings)
    application = FastAPI(title=settings.app_name, lifespan=lifespan)
    application.include_router(router)
    return application


app = create_app()
