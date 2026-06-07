from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
)

CurrencyCode = Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]


class EventRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "11111111-1111-1111-1111-111111111111",
                "user_id": "22222222-2222-2222-2222-222222222222",
                "amount": "100.25",
                "currency": "EUR",
                "timestamp": "2026-06-07T12:00:00Z",
            }
        }
    )

    id: UUID = Field(
        description="Unique event identifier used for idempotent processing.",
        examples=["11111111-1111-1111-1111-111111111111"],
    )
    user_id: UUID = Field(
        description="User who owns the transaction.",
        examples=["22222222-2222-2222-2222-222222222222"],
    )
    amount: Decimal = Field(
        gt=0,
        max_digits=20,
        decimal_places=8,
        description="Positive transaction amount in the original currency.",
        examples=["100.25"],
    )
    currency: CurrencyCode = Field(
        description="Uppercase three-letter source currency code.",
        examples=["EUR"],
    )
    timestamp: AwareDatetime = Field(
        description="Timezone-aware timestamp when the transaction occurred.",
        examples=["2026-06-07T12:00:00Z"],
    )

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


class QueuedEvent(EventRequest):
    version: Literal[1]


class EventAcceptedResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "11111111-1111-1111-1111-111111111111",
                "status": "accepted",
            }
        }
    )

    id: UUID = Field(
        description="Identifier of the event appended to Redis Streams.",
        examples=["11111111-1111-1111-1111-111111111111"],
    )
    status: Literal["accepted"] = Field(
        default="accepted",
        description="Confirms queue acceptance, not database persistence.",
        examples=["accepted"],
    )
