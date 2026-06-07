from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, Index, Numeric, String, func
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Transaction(Base):
    __tablename__ = "transactions"
    __table_args__ = (
        CheckConstraint(
            "original_amount > 0",
            name="ck_transactions_original_amount_positive",
        ),
        CheckConstraint(
            "char_length(original_currency) = 3",
            name="ck_transactions_currency_length",
        ),
        CheckConstraint(
            "original_currency = upper(original_currency)",
            name="ck_transactions_currency_uppercase",
        ),
        CheckConstraint("usd_rate > 0", name="ck_transactions_usd_rate_positive"),
        CheckConstraint(
            "amount_usd >= 0",
            name="ck_transactions_amount_usd_non_negative",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
    )
    user_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        nullable=False,
    )
    original_amount: Mapped[Decimal] = mapped_column(
        Numeric(20, 8),
        nullable=False,
    )
    original_currency: Mapped[str] = mapped_column(
        String(3),
        nullable=False,
    )
    usd_rate: Mapped[Decimal] = mapped_column(
        Numeric(20, 10),
        nullable=False,
    )
    amount_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 2),
        nullable=False,
    )
    event_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


Index(
    "ix_transactions_user_timestamp_id",
    Transaction.user_id,
    Transaction.event_timestamp.desc(),
    Transaction.id.desc(),
)
