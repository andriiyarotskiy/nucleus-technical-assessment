"""Create transactions table.

Revision ID: 20260607_01
Revises:
Create Date: 2026-06-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260607_01"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "transactions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("original_amount", sa.Numeric(20, 8), nullable=False),
        sa.Column("original_currency", sa.String(3), nullable=False),
        sa.Column("usd_rate", sa.Numeric(20, 10), nullable=False),
        sa.Column("amount_usd", sa.Numeric(20, 2), nullable=False),
        sa.Column("event_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "amount_usd >= 0",
            name="ck_transactions_amount_usd_non_negative",
        ),
        sa.CheckConstraint(
            "char_length(original_currency) = 3",
            name="ck_transactions_currency_length",
        ),
        sa.CheckConstraint(
            "original_currency = upper(original_currency)",
            name="ck_transactions_currency_uppercase",
        ),
        sa.CheckConstraint(
            "original_amount > 0",
            name="ck_transactions_original_amount_positive",
        ),
        sa.CheckConstraint(
            "usd_rate > 0",
            name="ck_transactions_usd_rate_positive",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_transactions_user_timestamp_id",
        "transactions",
        [
            "user_id",
            sa.literal_column("event_timestamp DESC"),
            sa.literal_column("id DESC"),
        ],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_transactions_user_timestamp_id",
        table_name="transactions",
    )
    op.drop_table("transactions")
