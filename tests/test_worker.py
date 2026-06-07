import asyncio
import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from redis.exceptions import ConnectionError, ResponseError
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from app.currency import (
    CurrencyConverter,
    FixedRateProvider,
    UnsupportedCurrencyError,
)
from app.db.models import Transaction
from app.metrics import EVENTS_DUPLICATE, EVENTS_FAILED, EVENTS_PROCESSED
from app.worker import (
    ProcessingOutcome,
    QueuedEvent,
    RedisStreamWorker,
    TransactionEventProcessor,
)


def make_event() -> QueuedEvent:
    return QueuedEvent(
        version=1,
        id=UUID("11111111-1111-1111-1111-111111111111"),
        user_id=UUID("22222222-2222-2222-2222-222222222222"),
        amount=Decimal("100.00"),
        currency="EUR",
        timestamp=datetime(2026, 6, 7, 12, tzinfo=UTC),
    )


def stream_fields(currency: str = "EUR") -> dict[str, str]:
    return {
        "data": json.dumps(
            {
                "version": 1,
                "id": "11111111-1111-1111-1111-111111111111",
                "user_id": "22222222-2222-2222-2222-222222222222",
                "amount": "100.00",
                "currency": currency,
                "timestamp": "2026-06-07T12:00:00Z",
            }
        )
    }


class RecordingRedis:
    def __init__(self, operations: list[str]) -> None:
        self.operations = operations
        self.group_create_calls: list[tuple[str, str, str, bool]] = []
        self.acknowledged_ids: list[str] = []
        self.ack_error: Exception | None = None
        self.read_response: list[tuple[str, list[tuple[str, dict[str, str]]]]] = []
        self.read_calls: list[tuple[str, str, dict[str, str], int, int]] = []
        self.claim_response: list[object] = ["0-0", [], []]
        self.claim_calls: list[tuple[str, str, str, int, str, int]] = []
        self.dead_letter_entries: list[tuple[str, dict[str, str]]] = []
        self.dead_letter_error: Exception | None = None

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str,
        mkstream: bool,
    ) -> object:
        self.group_create_calls.append((name, groupname, id, mkstream))
        return True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int,
        block: int,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        self.read_calls.append((groupname, consumername, streams, count, block))
        return self.read_response

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str,
        count: int,
    ) -> list[object]:
        self.claim_calls.append(
            (name, groupname, consumername, min_idle_time, start_id, count)
        )
        return self.claim_response

    async def xack(
        self,
        name: str,
        groupname: str,
        *ids: str,
    ) -> int:
        if self.ack_error is not None:
            raise self.ack_error
        self.operations.append("ack")
        self.acknowledged_ids.extend(ids)
        return len(ids)

    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
    ) -> str:
        if self.dead_letter_error is not None:
            raise self.dead_letter_error
        self.operations.append("dead_letter")
        self.dead_letter_entries.append((name, fields))
        return "2-0"


class RecordingProcessor:
    def __init__(
        self,
        operations: list[str],
        outcome: ProcessingOutcome,
    ) -> None:
        self.operations = operations
        self.outcome = outcome

    async def process(self, event: QueuedEvent) -> ProcessingOutcome:
        self.operations.append("commit")
        return self.outcome


class FailingProcessor:
    async def process(self, event: QueuedEvent) -> ProcessingOutcome:
        raise RuntimeError("database unavailable")


class UnsupportedCurrencyProcessor:
    async def process(self, event: QueuedEvent) -> ProcessingOutcome:
        raise UnsupportedCurrencyError(event.currency)


class RecordingMetrics:
    def __init__(self) -> None:
        self.increments: list[str] = []

    async def increment(self, metric_name: str) -> None:
        self.increments.append(metric_name)

    async def snapshot(self) -> object:
        raise NotImplementedError


def make_worker(
    redis: RecordingRedis,
    processor: RecordingProcessor | FailingProcessor | UnsupportedCurrencyProcessor,
    metrics: RecordingMetrics | None = None,
) -> RedisStreamWorker:
    return RedisStreamWorker(
        redis=redis,  # type: ignore[arg-type]
        processor=processor,
        metrics=metrics or RecordingMetrics(),  # type: ignore[arg-type]
        stream_name="transactions",
        group_name="transaction-processors",
        consumer_name="worker-1",
        dead_letter_stream="transactions:dead-letter",
        batch_size=10,
        block_ms=5_000,
        retry_delay_ms=5_000,
    )


@pytest.mark.parametrize(
    "outcome",
    [ProcessingOutcome.PROCESSED, ProcessingOutcome.DUPLICATE],
)
async def test_worker_acknowledges_only_after_successful_processing(
    outcome: ProcessingOutcome,
    caplog: pytest.LogCaptureFixture,
) -> None:
    operations: list[str] = []
    redis = RecordingRedis(operations)
    metrics = RecordingMetrics()
    worker = make_worker(
        redis,
        RecordingProcessor(operations, outcome),
        metrics,
    )

    with caplog.at_level(logging.INFO, logger="app.worker"):
        await worker.process_entry("1-0", stream_fields())

    assert operations == ["commit", "ack"]
    assert redis.acknowledged_ids == ["1-0"]
    assert f"transaction_{outcome}" in caplog.text
    assert metrics.increments == [
        EVENTS_PROCESSED if outcome is ProcessingOutcome.PROCESSED else EVENTS_DUPLICATE
    ]


async def test_worker_leaves_transient_failure_pending(
    caplog: pytest.LogCaptureFixture,
) -> None:
    redis = RecordingRedis([])
    metrics = RecordingMetrics()
    worker = make_worker(redis, FailingProcessor(), metrics)

    with caplog.at_level(logging.ERROR, logger="app.worker"):
        await worker.process_entry("1-0", stream_fields())

    assert redis.acknowledged_ids == []
    assert redis.dead_letter_entries == []
    assert "transaction_processing_failed" in caplog.text
    assert metrics.increments == [EVENTS_FAILED]


@pytest.mark.parametrize(
    ("fields", "reason"),
    [
        ({"data": "not-json"}, "invalid_payload"),
        (stream_fields("JPY"), "unsupported_currency"),
    ],
)
async def test_worker_dead_letters_permanent_failures(
    fields: dict[str, str],
    reason: str,
) -> None:
    operations: list[str] = []
    redis = RecordingRedis(operations)
    metrics = RecordingMetrics()
    processor = (
        UnsupportedCurrencyProcessor()
        if reason == "unsupported_currency"
        else RecordingProcessor(operations, ProcessingOutcome.PROCESSED)
    )
    worker = make_worker(redis, processor, metrics)

    await worker.process_entry("1-0", fields)

    assert operations == ["dead_letter", "ack"]
    assert redis.dead_letter_entries[0][1]["reason"] == reason
    assert metrics.increments == [EVENTS_FAILED]


async def test_dead_letter_failure_leaves_original_pending() -> None:
    redis = RecordingRedis([])
    redis.dead_letter_error = ConnectionError("Redis unavailable")
    worker = make_worker(redis, UnsupportedCurrencyProcessor())

    await worker.process_entry("1-0", stream_fields("JPY"))

    assert redis.acknowledged_ids == []


async def test_worker_creates_group_from_start_of_stream() -> None:
    redis = RecordingRedis([])
    worker = make_worker(
        redis,
        RecordingProcessor([], ProcessingOutcome.PROCESSED),
    )

    await worker.ensure_consumer_group()

    assert redis.group_create_calls == [
        ("transactions", "transaction-processors", "0-0", True)
    ]


async def test_existing_consumer_group_is_not_an_error() -> None:
    redis = RecordingRedis([])
    redis.xgroup_create = AsyncMock(
        side_effect=ResponseError("BUSYGROUP Consumer Group name already exists")
    )
    worker = make_worker(
        redis,
        RecordingProcessor([], ProcessingOutcome.PROCESSED),
    )

    await worker.ensure_consumer_group()


async def test_worker_reads_new_group_messages() -> None:
    operations: list[str] = []
    redis = RecordingRedis(operations)
    redis.read_response = [("transactions", [("1-0", stream_fields())])]
    worker = make_worker(
        redis,
        RecordingProcessor(operations, ProcessingOutcome.PROCESSED),
    )

    handled_count = await worker.read_new_once()

    assert handled_count == 1
    assert redis.read_calls == [
        (
            "transaction-processors",
            "worker-1",
            {"transactions": ">"},
            10,
            5_000,
        )
    ]
    assert operations == ["commit", "ack"]


async def test_worker_recovers_pending_messages_with_cursor() -> None:
    operations: list[str] = []
    redis = RecordingRedis(operations)
    redis.claim_response = ["5-0", [("1-0", stream_fields())], []]
    worker = make_worker(
        redis,
        RecordingProcessor(operations, ProcessingOutcome.DUPLICATE),
    )

    recovered_count = await worker.recover_pending_once()
    await worker.recover_pending_once()

    assert recovered_count == 1
    assert redis.claim_calls == [
        ("transactions", "transaction-processors", "worker-1", 5_000, "0-0", 10),
        ("transactions", "transaction-processors", "worker-1", 5_000, "5-0", 10),
    ]
    assert operations == ["commit", "ack", "commit", "ack"]


async def test_worker_retries_after_redis_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = RecordingRedis([])
    worker = make_worker(
        redis,
        RecordingProcessor([], ProcessingOutcome.PROCESSED),
    )
    worker.ensure_consumer_group = AsyncMock()
    worker.recover_pending_once = AsyncMock(side_effect=ConnectionError("down"))
    worker.read_new_once = AsyncMock(side_effect=asyncio.CancelledError)
    sleep = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", sleep)

    worker.recover_pending_once.side_effect = [
        ConnectionError("down"),
        asyncio.CancelledError(),
    ]
    with pytest.raises(asyncio.CancelledError):
        await worker.run_forever()

    sleep.assert_awaited_once_with(5)
    assert worker.ensure_consumer_group.await_count == 2


def session_factory(
    session: AsyncMock,
) -> MagicMock:
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=context)


async def test_processor_confirms_duplicate_before_conversion() -> None:
    session = AsyncMock(spec=AsyncSession)
    session.get.return_value = Transaction()
    converter = MagicMock(spec=CurrencyConverter)
    processor = TransactionEventProcessor(session_factory(session), converter)

    outcome = await processor.process(make_event())

    assert outcome is ProcessingOutcome.DUPLICATE
    converter.convert_to_usd.assert_not_called()
    session.execute.assert_not_awaited()


@pytest.mark.parametrize(
    ("inserted_id", "expected_outcome"),
    [
        (
            UUID("11111111-1111-1111-1111-111111111111"),
            ProcessingOutcome.PROCESSED,
        ),
        (None, ProcessingOutcome.DUPLICATE),
    ],
)
async def test_processor_uses_conflict_safe_insert(
    inserted_id: UUID | None,
    expected_outcome: ProcessingOutcome,
) -> None:
    duplicate_session = AsyncMock(spec=AsyncSession)
    duplicate_session.get.return_value = None

    insert_session = AsyncMock(spec=AsyncSession)
    result = MagicMock()
    result.scalar_one_or_none.return_value = inserted_id
    insert_session.execute.return_value = result
    transaction_context = MagicMock()
    transaction_context.__aenter__ = AsyncMock()
    transaction_context.__aexit__ = AsyncMock(return_value=False)
    insert_session.begin.return_value = transaction_context

    factory = MagicMock(
        side_effect=[
            session_factory(duplicate_session)(),
            session_factory(insert_session)(),
        ]
    )
    processor = TransactionEventProcessor(
        factory,
        CurrencyConverter(FixedRateProvider()),
    )

    outcome = await processor.process(make_event())

    assert outcome is expected_outcome
    statement = insert_session.execute.await_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "ON CONFLICT (id) DO NOTHING" in sql
    assert "RETURNING transactions.id" in sql
