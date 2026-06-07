from dataclasses import dataclass
from typing import Protocol, cast

from redis.asyncio import Redis

EVENTS_PROCESSED = "events_processed_total"
EVENTS_FAILED = "events_failed_total"
EVENTS_DUPLICATE = "events_duplicate_total"
METRIC_NAMES = (EVENTS_PROCESSED, EVENTS_FAILED, EVENTS_DUPLICATE)


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    events_processed_total: int
    events_failed_total: int
    events_duplicate_total: int


class MetricsStore(Protocol):
    async def increment(self, metric_name: str) -> None:
        """Increment one counter."""

    async def snapshot(self) -> MetricsSnapshot:
        """Return current counter values."""


class RedisMetricsClient(Protocol):
    async def hincrby(self, name: str, key: str, amount: int = 1) -> int:
        """Increment a Redis hash field."""

    async def hmget(self, name: str, keys: list[str]) -> list[str | None]:
        """Read Redis hash fields."""


class RedisMetricsStore:
    def __init__(self, redis: Redis, key: str) -> None:
        self._redis = cast(RedisMetricsClient, redis)
        self._key = key

    async def increment(self, metric_name: str) -> None:
        if metric_name not in METRIC_NAMES:
            raise ValueError(f"Unknown metric: {metric_name}")
        await self._redis.hincrby(self._key, metric_name, 1)

    async def snapshot(self) -> MetricsSnapshot:
        values = await self._redis.hmget(self._key, list(METRIC_NAMES))
        counters = [int(value or 0) for value in values]
        return MetricsSnapshot(*counters)


def render_prometheus(snapshot: MetricsSnapshot) -> str:
    lines = [
        "# HELP events_processed_total Events successfully stored.",
        "# TYPE events_processed_total counter",
        f"events_processed_total {snapshot.events_processed_total}",
        "# HELP events_failed_total Event processing attempts that failed.",
        "# TYPE events_failed_total counter",
        f"events_failed_total {snapshot.events_failed_total}",
        "# HELP events_duplicate_total Duplicate events confirmed in PostgreSQL.",
        "# TYPE events_duplicate_total counter",
        f"events_duplicate_total {snapshot.events_duplicate_total}",
    ]
    return "\n".join(lines) + "\n"
