from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from app.api import router
from app.config import get_settings
from app.events import RedisStreamEventProducer
from app.logging import configure_logging


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    application.state.event_producer = RedisStreamEventProducer(
        redis,
        settings.redis_stream_name,
    )
    yield
    await redis.aclose()


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings)
    application = FastAPI(title=settings.app_name, lifespan=lifespan)
    application.include_router(router)
    return application


app = create_app()
