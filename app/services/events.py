import json
from typing import Protocol

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.schemas.events import EventRequest


class EventPublishError(Exception):
    """Raised when an event cannot be appended to the stream."""


class EventProducer(Protocol):
    async def publish(self, event: EventRequest) -> str:
        """Publish an event and return its Redis Stream entry ID."""


class RedisStreamClient(Protocol):
    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
    ) -> str | bytes:
        """Append fields to a Redis Stream."""


class RedisStreamEventProducer:
    def __init__(
        self,
        redis: Redis | RedisStreamClient,
        stream_name: str,
    ) -> None:
        self._redis = redis
        self._stream_name = stream_name

    async def publish(self, event: EventRequest) -> str:
        payload = {
            "version": 1,
            "id": str(event.id),
            "user_id": str(event.user_id),
            "amount": str(event.amount),
            "currency": event.currency,
            "timestamp": event.timestamp.isoformat().replace("+00:00", "Z"),
        }

        try:
            entry_id = await self._redis.xadd(
                self._stream_name,
                {"data": json.dumps(payload, separators=(",", ":"))},
            )
        except RedisError as error:
            raise EventPublishError("Redis Stream publish failed") from error

        if isinstance(entry_id, bytes):
            return entry_id.decode()
        return entry_id
