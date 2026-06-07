from collections.abc import AsyncGenerator
from decimal import Decimal

import pytest
from httpx import ASGITransport, AsyncClient

from app.api import get_event_producer
from app.events import EventPublishError, EventRequest
from app.main import create_app

VALID_EVENT = {
    "id": "11111111-1111-1111-1111-111111111111",
    "user_id": "22222222-2222-2222-2222-222222222222",
    "amount": "100.25",
    "currency": "EUR",
    "timestamp": "2026-06-07T12:00:00+01:00",
}


class RecordingProducer:
    def __init__(self) -> None:
        self.published_event: EventRequest | None = None

    async def publish(self, event: EventRequest) -> str:
        self.published_event = event
        return "1-0"


class FailingProducer:
    async def publish(self, event: EventRequest) -> str:
        raise EventPublishError("Redis Stream publish failed")


@pytest.fixture
async def app_client() -> AsyncGenerator[tuple[AsyncClient, RecordingProducer]]:
    app = create_app()
    producer = RecordingProducer()
    app.dependency_overrides[get_event_producer] = lambda: producer

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client, producer


async def test_post_events_publishes_and_returns_202(
    app_client: tuple[AsyncClient, RecordingProducer],
) -> None:
    client, producer = app_client

    response = await client.post("/events", json=VALID_EVENT)

    assert response.status_code == 202
    assert response.json() == {
        "id": VALID_EVENT["id"],
        "status": "accepted",
    }
    assert producer.published_event is not None
    assert producer.published_event.amount == Decimal("100.25")
    assert producer.published_event.timestamp.isoformat() == "2026-06-07T11:00:00+00:00"


async def test_post_events_returns_503_when_publish_fails() -> None:
    app = create_app()
    app.dependency_overrides[get_event_producer] = lambda: FailingProducer()

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post("/events", json=VALID_EVENT)

    assert response.status_code == 503
    assert response.json() == {
        "detail": {
            "code": "event_publish_unavailable",
            "message": "Event queue is unavailable",
        }
    }


def test_post_events_openapi_documents_expected_responses() -> None:
    operation = create_app().openapi()["paths"]["/events"]["post"]

    assert set(operation["responses"]) == {"202", "422", "503"}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", "not-a-uuid"),
        ("amount", "0"),
        ("currency", "eur"),
        ("timestamp", "2026-06-07T12:00:00"),
    ],
)
async def test_post_events_rejects_invalid_payload(field: str, value: str) -> None:
    app = create_app()
    producer = RecordingProducer()
    app.dependency_overrides[get_event_producer] = lambda: producer
    payload = {**VALID_EVENT, field: value}

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post("/events", json=payload)

    assert response.status_code == 422
    assert producer.published_event is None
