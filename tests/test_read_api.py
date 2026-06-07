from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import OperationalError

from app.api import get_transaction_reader
from app.db.models import Transaction
from app.main import create_app
from app.transactions import TransactionPage, UserSummary

USER_ID = UUID("22222222-2222-2222-2222-222222222222")


class RecordingTransactionReader:
    def __init__(self) -> None:
        self.from_timestamp: datetime | None = None
        self.to_timestamp: datetime | None = None
        self.page: int | None = None
        self.limit: int | None = None

    async def get_user_summary(self, user_id: UUID) -> UserSummary:
        return UserSummary(
            total_usd=Decimal("208.50"),
            transaction_count=2,
        )

    async def list_user_transactions(
        self,
        user_id: UUID,
        from_timestamp: datetime | None,
        to_timestamp: datetime | None,
        page: int,
        limit: int,
    ) -> TransactionPage:
        self.from_timestamp = from_timestamp
        self.to_timestamp = to_timestamp
        self.page = page
        self.limit = limit
        return TransactionPage(
            items=[
                Transaction(
                    id=UUID("11111111-1111-1111-1111-111111111111"),
                    user_id=user_id,
                    original_amount=Decimal("100.00"),
                    original_currency="EUR",
                    usd_rate=Decimal("1.0800000000"),
                    amount_usd=Decimal("108.00"),
                    event_timestamp=datetime(2026, 6, 7, 12, tzinfo=UTC),
                    processed_at=datetime(2026, 6, 7, 12, 0, 1, tzinfo=UTC),
                )
            ],
            total=3,
        )


class FailingTransactionReader(RecordingTransactionReader):
    async def get_user_summary(self, user_id: UUID) -> UserSummary:
        raise OperationalError("select", {}, Exception("database unavailable"))


async def test_summary_returns_total_and_count() -> None:
    app = create_app()
    app.dependency_overrides[get_transaction_reader] = RecordingTransactionReader

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(f"/users/{USER_ID}/summary")

    assert response.status_code == 200
    assert response.json() == {
        "user_id": str(USER_ID),
        "total_usd": "208.50",
        "transaction_count": 2,
    }


async def test_transactions_apply_filters_and_pagination() -> None:
    app = create_app()
    reader = RecordingTransactionReader()
    app.dependency_overrides[get_transaction_reader] = lambda: reader

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            f"/users/{USER_ID}/transactions",
            params={
                "from": "2026-06-01T00:00:00Z",
                "to": "2026-06-30T23:59:59Z",
                "page": 2,
                "limit": 1,
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "user_id": str(USER_ID),
                "original_amount": "100.00",
                "original_currency": "EUR",
                "usd_rate": "1.0800000000",
                "amount_usd": "108.00",
                "event_timestamp": "2026-06-07T12:00:00Z",
                "processed_at": "2026-06-07T12:00:01Z",
            }
        ],
        "page": 2,
        "limit": 1,
        "total": 3,
        "has_more": True,
    }
    assert reader.from_timestamp == datetime(2026, 6, 1, tzinfo=UTC)
    assert reader.to_timestamp == datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC)
    assert reader.page == 2
    assert reader.limit == 1


@pytest.mark.parametrize(
    "params",
    [
        {"page": 0},
        {"page": 1_000_001},
        {"limit": 0},
        {"limit": 101},
        {"from": "2026-06-07T12:00:00"},
        {
            "from": "2026-06-08T00:00:00Z",
            "to": "2026-06-07T00:00:00Z",
        },
    ],
)
async def test_transactions_reject_invalid_query_parameters(
    params: dict[str, str | int],
) -> None:
    app = create_app()
    app.dependency_overrides[get_transaction_reader] = RecordingTransactionReader

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            f"/users/{USER_ID}/transactions",
            params=params,
        )

    assert response.status_code == 422


async def test_summary_returns_503_when_database_fails() -> None:
    app = create_app()
    app.dependency_overrides[get_transaction_reader] = FailingTransactionReader

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(f"/users/{USER_ID}/summary")

    assert response.status_code == 503
    assert response.json() == {
        "detail": {
            "code": "database_unavailable",
            "message": "Database is unavailable",
        }
    }


def test_read_api_openapi_documents_expected_responses() -> None:
    paths = create_app().openapi()["paths"]

    assert set(paths["/users/{user_id}/summary"]["get"]["responses"]) == {
        "200",
        "422",
        "503",
    }
    assert set(paths["/users/{user_id}/transactions"]["get"]["responses"]) == {
        "200",
        "422",
        "503",
    }
