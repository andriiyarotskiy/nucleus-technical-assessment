from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased

from app.db.models import Transaction
from app.db.session import AsyncSessionFactory


@dataclass(frozen=True, slots=True)
class TransactionInsert:
    id: UUID
    user_id: UUID
    original_amount: Decimal
    original_currency: str
    usd_rate: Decimal
    amount_usd: Decimal
    event_timestamp: datetime


@dataclass(frozen=True, slots=True)
class UserSummary:
    total_usd: Decimal
    transaction_count: int


@dataclass(frozen=True, slots=True)
class TransactionPage:
    items: list[Transaction]
    total: int


class TransactionRepository:
    def __init__(self, session_factory: AsyncSessionFactory) -> None:
        self._session_factory = session_factory

    async def exists(self, transaction_id: UUID) -> bool:
        async with self._session_factory() as session:
            return await session.get(Transaction, transaction_id) is not None

    async def insert_if_absent(self, record: TransactionInsert) -> bool:
        statement = (
            insert(Transaction)
            .values(
                id=record.id,
                user_id=record.user_id,
                original_amount=record.original_amount,
                original_currency=record.original_currency,
                usd_rate=record.usd_rate,
                amount_usd=record.amount_usd,
                event_timestamp=record.event_timestamp,
            )
            .on_conflict_do_nothing(index_elements=[Transaction.id])
            .returning(Transaction.id)
        )

        async with self._session_factory() as session:
            async with session.begin():
                inserted_id = (await session.execute(statement)).scalar_one_or_none()

        return inserted_id is not None

    async def get_user_summary(self, user_id: UUID) -> UserSummary:
        statement = select(
            func.coalesce(func.sum(Transaction.amount_usd), Decimal("0.00")),
            func.count(Transaction.id),
        ).where(Transaction.user_id == user_id)

        async with self._session_factory() as session:
            result = await session.execute(statement)
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

        async with self._session_factory() as session:
            rows = (await session.execute(statement)).all()

        return TransactionPage(
            items=[
                stored_transaction
                for stored_transaction, _ in rows
                if stored_transaction is not None
            ],
            total=int(rows[0][1]),
        )
