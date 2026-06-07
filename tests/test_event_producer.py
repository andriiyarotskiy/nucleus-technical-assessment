import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest
from redis.exceptions import ConnectionError

from app.events import EventPublishError, EventRequest, RedisStreamEventProducer


class RecordingRedis:
    def __init__(self) -> None:
        self.stream_name: str | None = None
        self.fields: dict[str, str] | None = None

    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
    ) -> str:
        self.stream_name = name
        self.fields = fields
        return "1-0"


class FailingRedis:
    async def xadd(self, name: str, fields: dict[str, str]) -> str:
        raise ConnectionError("Redis unavailable")


def make_event() -> EventRequest:
    return EventRequest(
        id=UUID("11111111-1111-1111-1111-111111111111"),
        user_id=UUID("22222222-2222-2222-2222-222222222222"),
        amount=Decimal("100.25"),
        currency="EUR",
        timestamp=datetime(2026, 6, 7, 11, tzinfo=UTC),
    )


async def test_producer_appends_versioned_json_to_stream() -> None:
    redis = RecordingRedis()
    producer = RedisStreamEventProducer(redis, "transactions")

    entry_id = await producer.publish(make_event())

    assert entry_id == "1-0"
    assert redis.stream_name == "transactions"
    assert redis.fields is not None
    payload: dict[str, Any] = json.loads(redis.fields["data"])
    assert payload == {
        "version": 1,
        "id": "11111111-1111-1111-1111-111111111111",
        "user_id": "22222222-2222-2222-2222-222222222222",
        "amount": "100.25",
        "currency": "EUR",
        "timestamp": "2026-06-07T11:00:00Z",
    }


async def test_producer_translates_redis_failure() -> None:
    producer = RedisStreamEventProducer(FailingRedis(), "transactions")

    with pytest.raises(EventPublishError, match="Redis Stream publish failed"):
        await producer.publish(make_event())
