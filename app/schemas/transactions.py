from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

DecimalString = Annotated[
    Decimal,
    PlainSerializer(lambda value: format(value, "f"), return_type=str),
]


class UserSummaryResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "user_id": "22222222-2222-2222-2222-222222222222",
                "total_usd": "208.50",
                "transaction_count": 2,
            }
        }
    )

    user_id: UUID = Field(
        description="User whose transactions were aggregated.",
        examples=["22222222-2222-2222-2222-222222222222"],
    )
    total_usd: DecimalString = Field(
        description="Sum of persisted USD amounts, serialized as a decimal string.",
        examples=["208.50"],
    )
    transaction_count: int = Field(
        description="Number of persisted transactions for the user.",
        examples=[2],
    )


class TransactionResponse(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": "11111111-1111-1111-1111-111111111111",
                "user_id": "22222222-2222-2222-2222-222222222222",
                "original_amount": "100.00",
                "original_currency": "EUR",
                "usd_rate": "1.0800000000",
                "amount_usd": "108.00",
                "event_timestamp": "2026-06-07T12:00:00Z",
                "processed_at": "2026-06-07T12:00:01Z",
            }
        },
    )

    id: UUID = Field(description="Source event ID and database deduplication key.")
    user_id: UUID = Field(description="User who owns the transaction.")
    original_amount: DecimalString = Field(
        description="Amount received in the source event."
    )
    original_currency: str = Field(description="Source currency code.")
    usd_rate: DecimalString = Field(description="USD conversion rate that was applied.")
    amount_usd: DecimalString = Field(
        description="Converted amount rounded to two decimal places."
    )
    event_timestamp: datetime = Field(
        description="UTC-normalized timestamp supplied by the producer."
    )
    processed_at: datetime = Field(
        description="Timestamp when PostgreSQL persisted the transaction."
    )


class TransactionPageResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "items": [
                    {
                        "id": "11111111-1111-1111-1111-111111111111",
                        "user_id": "22222222-2222-2222-2222-222222222222",
                        "original_amount": "100.00",
                        "original_currency": "EUR",
                        "usd_rate": "1.0800000000",
                        "amount_usd": "108.00",
                        "event_timestamp": "2026-06-07T12:00:00Z",
                        "processed_at": "2026-06-07T12:00:01Z",
                    }
                ],
                "page": 1,
                "limit": 50,
                "total": 1,
                "has_more": False,
            }
        }
    )

    items: list[TransactionResponse] = Field(
        description="Transactions ordered by event timestamp descending."
    )
    page: int = Field(description="Current one-based page number.", examples=[1])
    limit: int = Field(description="Maximum items returned per page.", examples=[50])
    total: int = Field(
        description="Total matching transactions across all pages.",
        examples=[1],
    )
    has_more: bool = Field(
        description="Whether a subsequent page exists.",
        examples=[False],
    )
