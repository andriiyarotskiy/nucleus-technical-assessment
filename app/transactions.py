from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, PlainSerializer
from sqlalchemy import func, select, true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.db.models import Transaction

DecimalString = Annotated[
    Decimal,
    PlainSerializer(lambda value: format(value, "f"), return_type=str),
]


class UserSummaryResponse(BaseModel):
    user_id: UUID
    total_usd: DecimalString
    transaction_count: int


class TransactionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    original_amount: DecimalString
    original_currency: str
    usd_rate: DecimalString
    amount_usd: DecimalString
    event_timestamp: datetime
    processed_at: datetime


class TransactionPageResponse(BaseModel):
    items: list[TransactionResponse]
    page: int
    limit: int
    total: int
    has_more: bool


@dataclass(frozen=True, slots=True)
class UserSummary:
    total_usd: Decimal
    transaction_count: int


@dataclass(frozen=True, slots=True)
class TransactionPage:
    items: list[Transaction]
    total: int


class TransactionReader(Protocol):
    async def get_user_summary(self, user_id: UUID) -> UserSummary:
        """Return the stored USD total and transaction count for a user."""

    async def list_user_transactions(
        self,
        user_id: UUID,
        from_timestamp: datetime | None,
        to_timestamp: datetime | None,
        page: int,
        limit: int,
    ) -> TransactionPage:
        """Return one page of a user's stored transactions."""


class SQLAlchemyTransactionReader:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_user_summary(self, user_id: UUID) -> UserSummary:
        statement = select(
            func.coalesce(func.sum(Transaction.amount_usd), Decimal("0.00")),
            func.count(Transaction.id),
        ).where(Transaction.user_id == user_id)

        result = await self._session.execute(statement)
        total_usd, transaction_count = result.one()
        return UserSummary(
            total_usd=Decimal(total_usd),
            transaction_count=int(transaction_count),
        )

    async def list_user_transactions(
        self,
        user_id: UUID,
        from_timestamp: datetime | None,
        to_timestamp: datetime | None,
        page: int,
        limit: int,
    ) -> TransactionPage:
        filters = [Transaction.user_id == user_id]
        if from_timestamp is not None:
            filters.append(Transaction.event_timestamp >= from_timestamp)
        if to_timestamp is not None:
            filters.append(Transaction.event_timestamp <= to_timestamp)

        filtered_transactions = (
            select(Transaction).where(*filters).cte("filtered_transactions")
        )
        page_rows = (
            select(filtered_transactions)
            .order_by(
                filtered_transactions.c.event_timestamp.desc(),
                filtered_transactions.c.id.desc(),
            )
            .offset((page - 1) * limit)
            .limit(limit)
            .cte("page_rows")
        )
        total_rows = (
            select(func.count().label("total"))
            .select_from(filtered_transactions)
            .cte("total_rows")
        )
        transaction = aliased(Transaction, page_rows)
        statement = (
            select(transaction, total_rows.c.total)
            .select_from(total_rows.outerjoin(page_rows, true()))
            .order_by(page_rows.c.event_timestamp.desc(), page_rows.c.id.desc())
        )
        rows = (await self._session.execute(statement)).all()

        return TransactionPage(
            items=[
                stored_transaction
                for stored_transaction, _ in rows
                if stored_transaction is not None
            ],
            total=int(rows[0][1]),
        )
