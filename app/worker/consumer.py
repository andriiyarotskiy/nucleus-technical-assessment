import asyncio
import logging

from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from app.schemas.events import QueuedEvent
from app.services.currency import UnsupportedCurrencyError
from app.services.metrics import (
    EVENTS_DUPLICATE,
    EVENTS_FAILED,
    EVENTS_PROCESSED,
    MetricsStore,
)
from app.services.transactions import EventProcessor, ProcessingOutcome

logger = logging.getLogger(__name__)

StreamFields = dict[str, str]
StreamMessage = tuple[str, StreamFields]


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
