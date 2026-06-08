import json
from collections import deque
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from app.schemas.events import QueuedEvent
from app.services.metrics import EVENTS_FAILED, EVENTS_PROCESSED, MetricsSnapshot
from app.services.transactions import ProcessingOutcome
from app.worker.consumer import PendingEntry, RedisStreamWorker, StreamFields


def make_payload() -> str:
    event = QueuedEvent(
        version=1,
        id=UUID("11111111-1111-1111-1111-111111111111"),
        user_id=UUID("22222222-2222-2222-2222-222222222222"),
        amount=Decimal("100.00"),
        currency="EUR",
        timestamp=datetime(2026, 6, 7, 12, tzinfo=UTC),
    )
    return json.dumps(
        {
            "version": event.version,
            "id": str(event.id),
            "user_id": str(event.user_id),
            "amount": str(event.amount),
            "currency": event.currency,
            "timestamp": event.timestamp.isoformat().replace("+00:00", "Z"),
        }
    )


class FakeRedis:
    def __init__(self, recorder: list[str] | None = None) -> None:
        self.recorder = recorder if recorder is not None else []
        self.acks: list[tuple[str, str, str]] = []
        self.dead_letters: list[tuple[str, dict[str, str]]] = []
        self.pending_entries: list[PendingEntry] = []
        self.claimed_messages: dict[str, StreamFields] = {}
        self.xclaim_calls: list[tuple[int, tuple[str, ...]]] = []

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str,
        mkstream: bool,
    ) -> None:
        return None

    async def xack(self, name: str, groupname: str, stream_id: str) -> int:
        self.recorder.append("xack")
        self.acks.append((name, groupname, stream_id))
        return 1

    async def xadd(self, name: str, fields: dict[str, str]) -> str:
        self.dead_letters.append((name, fields))
        return "dead-letter-1"

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int,
        block: int,
    ) -> list[tuple[str, list[tuple[str, StreamFields]]]]:
        return []

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
        filtered = self.pending_entries
        if idle is not None:
            filtered = [
                entry for entry in filtered if entry["time_since_delivered"] >= idle
            ]
        return filtered[:count]

    async def xclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        message_ids: tuple[str, ...],
    ) -> list[tuple[str, StreamFields]]:
        self.xclaim_calls.append((min_idle_time, message_ids))
        return [
            (message_id, self.claimed_messages[message_id])
            for message_id in message_ids
        ]


class FakeMetrics:
    def __init__(self, recorder: list[str] | None = None) -> None:
        self.recorder = recorder if recorder is not None else []
        self.incremented: list[str] = []

    async def increment(self, metric_name: str) -> None:
        self.recorder.append(f"metric:{metric_name}")
        self.incremented.append(metric_name)

    async def snapshot(self) -> MetricsSnapshot:
        return MetricsSnapshot(0, 0, 0)


class SequenceProcessor:
    def __init__(
        self,
        outcomes: list[ProcessingOutcome | Exception],
        recorder: list[str],
    ) -> None:
        self._outcomes = deque(outcomes)
        self.recorder = recorder

    async def process(self, event: QueuedEvent) -> ProcessingOutcome:
        self.recorder.append("process")
        outcome = self._outcomes.popleft()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def make_worker(
    redis: FakeRedis,
    processor: SequenceProcessor,
    metrics: FakeMetrics,
) -> RedisStreamWorker:
    return RedisStreamWorker(
        redis=redis,
        processor=processor,
        metrics=metrics,
        stream_name="transactions",
        group_name="transaction-processors",
        consumer_name="worker-1",
        dead_letter_stream="transactions:dead-letter",
        batch_size=10,
        block_ms=100,
        retry_base_delay_ms=1_000,
        retry_max_delay_ms=4_000,
    )


async def test_process_entry_acks_only_after_successful_processing() -> None:
    recorder: list[str] = []
    redis = FakeRedis(recorder)
    metrics = FakeMetrics(recorder)
    processor = SequenceProcessor([ProcessingOutcome.PROCESSED], recorder)
    worker = make_worker(redis, processor, metrics)

    await worker.process_entry("1-0", {"data": make_payload()})

    assert redis.acks == [("transactions", "transaction-processors", "1-0")]
    assert recorder == ["process", "xack", f"metric:{EVENTS_PROCESSED}"]


async def test_process_entry_does_not_ack_on_retryable_failure() -> None:
    recorder: list[str] = []
    redis = FakeRedis(recorder)
    metrics = FakeMetrics(recorder)
    processor = SequenceProcessor([RuntimeError("database unavailable")], recorder)
    worker = make_worker(redis, processor, metrics)

    await worker.process_entry("1-0", {"data": make_payload()})

    assert redis.acks == []
    assert metrics.incremented == [EVENTS_FAILED]


async def test_pending_recovery_applies_backoff_and_resets_after_success() -> None:
    recorder: list[str] = []
    redis = FakeRedis(recorder)
    metrics = FakeMetrics(recorder)
    processor = SequenceProcessor(
        [
            RuntimeError("database unavailable"),
            RuntimeError("database unavailable"),
            ProcessingOutcome.PROCESSED,
        ],
        recorder,
    )
    worker = make_worker(redis, processor, metrics)
    payload = make_payload()
    redis.claimed_messages["1-0"] = {"data": payload}

    await worker.process_entry("1-0", {"data": payload})

    redis.pending_entries = [
        PendingEntry(
            message_id="1-0",
            consumer="worker-old",
            time_since_delivered=999,
            times_delivered=1,
        )
    ]
    assert await worker.recover_pending_once() == 0
    assert redis.xclaim_calls == []

    redis.pending_entries[0]["time_since_delivered"] = 1_000
    assert await worker.recover_pending_once() == 1
    assert redis.xclaim_calls == [(1_000, ("1-0",))]

    redis.pending_entries[0]["time_since_delivered"] = 1_999
    assert await worker.recover_pending_once() == 0

    redis.pending_entries[0]["time_since_delivered"] = 2_000
    assert await worker.recover_pending_once() == 1
    assert redis.xclaim_calls == [(1_000, ("1-0",)), (2_000, ("1-0",))]
    assert redis.acks == [("transactions", "transaction-processors", "1-0")]
    assert worker._required_retry_delay_ms("1-0") == 1_000
