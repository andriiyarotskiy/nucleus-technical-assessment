import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Protocol, TypedDict, cast

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


class PendingEntry(TypedDict):
    message_id: str
    consumer: str
    time_since_delivered: int
    times_delivered: int


class RedisStreamClient(Protocol):
    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str,
        mkstream: bool,
    ) -> None:
        """Create a consumer group."""

    async def xack(self, name: str, groupname: str, stream_id: str) -> int:
        """Acknowledge one stream entry."""

    async def xadd(self, name: str, fields: dict[str, str]) -> str:
        """Append one stream entry."""

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int,
        block: int,
    ) -> list[tuple[str, list[StreamMessage]]]:
        """Read new consumer-group messages."""

    async def xpending_range(
        self,
        name: str,
        groupname: str,
        min: str,
        max: str,
        count: int,
        consumername: str | None = None,
        idle: int | None = None,
    ) -> list[PendingEntry]:
        """Read pending-message details."""

    async def xclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        message_ids: tuple[str, ...],
    ) -> list[StreamMessage]:
        """Claim pending entries for a consumer."""


@dataclass(slots=True)
class RetryPolicy:
    base_delay_ms: int
    max_delay_ms: int

    def delay_for_attempt(self, attempt: int) -> int:
        exponent: int = max(attempt - 1, 0)
        delay_ms: int = self.base_delay_ms * (2**exponent)
        if delay_ms > self.max_delay_ms:
            return self.max_delay_ms
        return delay_ms


class RedisStreamWorker:
    def __init__(
        self,
        redis: Redis | RedisStreamClient,
        processor: EventProcessor,
        metrics: MetricsStore,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        dead_letter_stream: str,
        batch_size: int,
        block_ms: int,
        retry_base_delay_ms: int,
        retry_max_delay_ms: int,
    ) -> None:
        self._redis = cast(RedisStreamClient, redis)
        self._processor = processor
        self._metrics = metrics
        self._stream_name = stream_name
        self._group_name = group_name
        self._consumer_name = consumer_name
        self._dead_letter_stream = dead_letter_stream
        self._batch_size = batch_size
        self._block_ms = block_ms
        self._retry_policy = RetryPolicy(
            base_delay_ms=retry_base_delay_ms,
            max_delay_ms=retry_max_delay_ms,
        )
        self._retry_attempts: dict[str, int] = {}

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
            self._mark_retry(stream_id)
            logger.exception("transaction_processing_failed stream_id=%s", stream_id)
            await self._record_metric(EVENTS_FAILED)
            return

        self._clear_retry(stream_id)
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

        self._clear_retry(stream_id)
        logger.error(
            "transaction_dead_lettered stream_id=%s reason=%s stream=%s",
            stream_id,
            reason,
            self._dead_letter_stream,
        )

    def _mark_retry(self, stream_id: str) -> None:
        self._retry_attempts[stream_id] = self._retry_attempts.get(stream_id, 0) + 1

    def _clear_retry(self, stream_id: str) -> None:
        self._retry_attempts.pop(stream_id, None)

    def _required_retry_delay_ms(self, stream_id: str) -> int:
        attempt = self._retry_attempts.get(stream_id, 1)
        return self._retry_policy.delay_for_attempt(attempt)

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
        pending_entries = await self._redis.xpending_range(
            self._stream_name,
            self._group_name,
            min="-",
            max="+",
            count=self._batch_size * 10,
            idle=self._retry_policy.base_delay_ms,
        )
        due_entry_ids_by_delay: dict[int, list[str]] = defaultdict(list)
        due_count = 0
        for entry in pending_entries:
            stream_id = entry["message_id"]
            required_delay_ms = self._required_retry_delay_ms(stream_id)
            if entry["time_since_delivered"] < required_delay_ms:
                continue
            due_entry_ids_by_delay[required_delay_ms].append(stream_id)
            due_count += 1
            if due_count >= self._batch_size:
                break

        processed_count = 0
        for required_delay_ms, stream_ids in sorted(due_entry_ids_by_delay.items()):
            claimed_messages = await self._redis.xclaim(
                self._stream_name,
                self._group_name,
                self._consumer_name,
                min_idle_time=required_delay_ms,
                message_ids=tuple(stream_ids),
            )
            processed_count += await self._process_messages(
                [(self._stream_name, claimed_messages)],
            )
        return processed_count

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
                await asyncio.sleep(self._retry_policy.base_delay_ms / 1_000)
