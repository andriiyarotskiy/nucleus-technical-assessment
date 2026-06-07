import asyncio
import logging
import socket
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError
from sqlalchemy.dialects.postgresql import insert

from app.config import Settings, get_settings
from app.currency import CurrencyConverter, FixedRateProvider, UnsupportedCurrencyError
from app.db.models import Transaction
from app.db.session import (
    AsyncSessionFactory,
    build_async_engine,
    build_session_factory,
)
from app.events import EventRequest
from app.logging import configure_logging
from app.metrics import (
    EVENTS_DUPLICATE,
    EVENTS_FAILED,
    EVENTS_PROCESSED,
    MetricsStore,
    RedisMetricsStore,
)

logger = logging.getLogger(__name__)

StreamFields = dict[str, str]
StreamMessage = tuple[str, StreamFields]


class QueuedEvent(EventRequest):
    version: Literal[1]


class ProcessingOutcome(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"


class EventProcessor(Protocol):
    async def process(self, event: QueuedEvent) -> ProcessingOutcome:
        """Persist one event and return whether it was newly inserted."""


class TransactionEventProcessor:
    def __init__(
        self,
        session_factory: AsyncSessionFactory,
        converter: CurrencyConverter,
    ) -> None:
        self._session_factory = session_factory
        self._converter = converter

    async def process(self, event: QueuedEvent) -> ProcessingOutcome:
        async with self._session_factory() as session:
            if await session.get(Transaction, event.id) is not None:
                return ProcessingOutcome.DUPLICATE

        conversion = await self._converter.convert_to_usd(
            event.amount,
            event.currency,
        )
        statement = (
            insert(Transaction)
            .values(
                id=event.id,
                user_id=event.user_id,
                original_amount=event.amount,
                original_currency=event.currency,
                usd_rate=conversion.usd_rate,
                amount_usd=conversion.amount_usd,
                event_timestamp=event.timestamp,
            )
            .on_conflict_do_nothing(index_elements=[Transaction.id])
            .returning(Transaction.id)
        )

        async with self._session_factory() as session:
            async with session.begin():
                inserted_id = (await session.execute(statement)).scalar_one_or_none()

        if inserted_id is None:
            return ProcessingOutcome.DUPLICATE
        return ProcessingOutcome.PROCESSED


class RedisStreamWorker:
    def __init__(
        self,
        redis: Redis,
        processor: EventProcessor,
        metrics: MetricsStore,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        dead_letter_stream: str,
        batch_size: int,
        block_ms: int,
        retry_delay_ms: int,
    ) -> None:
        self._redis = redis
        self._processor = processor
        self._metrics = metrics
        self._stream_name = stream_name
        self._group_name = group_name
        self._consumer_name = consumer_name
        self._dead_letter_stream = dead_letter_stream
        self._batch_size = batch_size
        self._block_ms = block_ms
        self._retry_delay_ms = retry_delay_ms
        self._claim_cursor = "0-0"

    async def ensure_consumer_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                self._stream_name,
                self._group_name,
                id="0-0",
                mkstream=True,
            )
            logger.info(
                "consumer_group_created stream=%s group=%s",
                self._stream_name,
                self._group_name,
            )
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise

    async def process_entry(self, stream_id: str, fields: StreamFields) -> None:
        payload = fields.get("data")
        if payload is None:
            await self._record_metric(EVENTS_FAILED)
            await self._dead_letter(stream_id, "", "invalid_payload")
            return

        try:
            event = QueuedEvent.model_validate_json(payload)
            outcome = await self._processor.process(event)
        except ValidationError:
            logger.exception("transaction_payload_invalid stream_id=%s", stream_id)
            await self._record_metric(EVENTS_FAILED)
            await self._dead_letter(stream_id, payload, "invalid_payload")
            return
        except UnsupportedCurrencyError:
            logger.exception("transaction_currency_unsupported stream_id=%s", stream_id)
            await self._record_metric(EVENTS_FAILED)
            await self._dead_letter(stream_id, payload, "unsupported_currency")
            return
        except Exception:
            logger.exception("transaction_processing_failed stream_id=%s", stream_id)
            await self._record_metric(EVENTS_FAILED)
            return

        try:
            await self._redis.xack(self._stream_name, self._group_name, stream_id)
        except RedisError:
            logger.exception(
                "transaction_ack_failed outcome=%s event_id=%s stream_id=%s",
                outcome,
                event.id,
                stream_id,
            )
            return

        metric_name = (
            EVENTS_PROCESSED
            if outcome is ProcessingOutcome.PROCESSED
            else EVENTS_DUPLICATE
        )
        await self._record_metric(metric_name)
        logger.info(
            "transaction_%s event_id=%s stream_id=%s",
            outcome,
            event.id,
            stream_id,
        )

    async def _record_metric(self, metric_name: str) -> None:
        try:
            await self._metrics.increment(metric_name)
        except RedisError:
            logger.exception("metric_increment_failed metric=%s", metric_name)

    async def _dead_letter(
        self,
        stream_id: str,
        payload: str,
        reason: str,
    ) -> None:
        try:
            await self._redis.xadd(
                self._dead_letter_stream,
                {
                    "data": payload,
                    "source_stream": self._stream_name,
                    "source_id": stream_id,
                    "reason": reason,
                },
            )
            await self._redis.xack(
                self._stream_name,
                self._group_name,
                stream_id,
            )
        except RedisError:
            logger.exception(
                "transaction_dead_letter_failed stream_id=%s reason=%s",
                stream_id,
                reason,
            )
            return

        logger.error(
            "transaction_dead_lettered stream_id=%s reason=%s stream=%s",
            stream_id,
            reason,
            self._dead_letter_stream,
        )

    async def read_new_once(self) -> int:
        response = await self._redis.xreadgroup(
            self._group_name,
            self._consumer_name,
            streams={self._stream_name: ">"},
            count=self._batch_size,
            block=self._block_ms,
        )
        return await self._process_messages(response)

    async def recover_pending_once(self) -> int:
        response = await self._redis.xautoclaim(
            self._stream_name,
            self._group_name,
            self._consumer_name,
            min_idle_time=self._retry_delay_ms,
            start_id=self._claim_cursor,
            count=self._batch_size,
        )
        self._claim_cursor = response[0]
        return await self._process_messages(
            [(self._stream_name, response[1])],
        )

    async def _process_messages(
        self,
        response: list[tuple[str, list[StreamMessage]]],
    ) -> int:
        processed_count = 0
        for _, messages in response:
            for stream_id, fields in messages:
                await self.process_entry(stream_id, fields)
                processed_count += 1
        return processed_count

    async def run_forever(self) -> None:
        while True:
            try:
                await self.ensure_consumer_group()
                logger.info(
                    "worker_started stream=%s group=%s consumer=%s",
                    self._stream_name,
                    self._group_name,
                    self._consumer_name,
                )
                while True:
                    await self.recover_pending_once()
                    await self.read_new_once()
            except RedisError:
                logger.exception("worker_redis_unavailable")
                await asyncio.sleep(self._retry_delay_ms / 1_000)


async def run_worker(settings: Settings) -> None:
    configure_logging(settings)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    database_engine = build_async_engine(settings.postgres_dsn)
    processor = TransactionEventProcessor(
        build_session_factory(database_engine),
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


if __name__ == "__main__":
    main()
