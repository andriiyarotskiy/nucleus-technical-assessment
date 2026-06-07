import asyncio
import socket

from redis.asyncio import Redis

from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.repositories.transactions import TransactionRepository
from app.db.session import build_async_engine, build_session_factory
from app.services.currency import CurrencyConverter, FixedRateProvider
from app.services.metrics import RedisMetricsStore
from app.services.transactions import TransactionEventProcessor
from app.worker.consumer import RedisStreamWorker


async def run_worker(settings: Settings) -> None:
    configure_logging(settings)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    database_engine = build_async_engine(settings.postgres_dsn)
    transactions = TransactionRepository(build_session_factory(database_engine))
    processor = TransactionEventProcessor(
        transactions,
        CurrencyConverter(FixedRateProvider()),
    )
    worker = RedisStreamWorker(
        redis=redis,
        processor=processor,
        metrics=RedisMetricsStore(redis, settings.redis_metrics_key),
        stream_name=settings.redis_stream_name,
        group_name=settings.redis_consumer_group,
        consumer_name=settings.redis_consumer_name or f"worker-{socket.gethostname()}",
        dead_letter_stream=settings.redis_dead_letter_stream,
        batch_size=settings.redis_batch_size,
        block_ms=settings.redis_block_ms,
        retry_delay_ms=settings.worker_retry_delay_ms,
    )

    try:
        await worker.run_forever()
    finally:
        await redis.aclose()
        await database_engine.dispose()


def main() -> None:
    asyncio.run(run_worker(get_settings()))
