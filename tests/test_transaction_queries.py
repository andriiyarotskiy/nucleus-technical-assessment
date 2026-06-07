from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Transaction
from app.transactions import SQLAlchemyTransactionReader

USER_ID = UUID("22222222-2222-2222-2222-222222222222")


async def test_summary_query_aggregates_for_one_user() -> None:
    session = AsyncMock(spec=AsyncSession)
    result = MagicMock()
    result.one.return_value = (Decimal("108.00"), 1)
    session.execute.return_value = result
    reader = SQLAlchemyTransactionReader(session)

    summary = await reader.get_user_summary(USER_ID)

    assert summary.total_usd == Decimal("108.00")
    assert summary.transaction_count == 1
    statement = session.execute.await_args.args[0]
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "sum(transactions.amount_usd)" in sql
    assert "count(transactions.id)" in sql
    assert f"transactions.user_id = '{USER_ID}'" in sql


async def test_list_query_filters_orders_and_paginates() -> None:
    session = AsyncMock(spec=AsyncSession)
    result = MagicMock()
    transaction = MagicMock(spec=Transaction)
    result.all.return_value = [(transaction, 1)]
    session.execute.return_value = result
    reader = SQLAlchemyTransactionReader(session)
    from_timestamp = datetime(2026, 6, 1, tzinfo=UTC)
    to_timestamp = datetime(2026, 6, 30, tzinfo=UTC)

    page = await reader.list_user_transactions(
        user_id=USER_ID,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        page=3,
        limit=25,
    )

    assert page.items == [transaction]
    assert page.total == 1
    statement = session.execute.await_args.args[0]
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert f"transactions.user_id = '{USER_ID}'" in sql
    assert "transactions.event_timestamp >= '2026-06-01 00:00:00+00:00'" in sql
    assert "transactions.event_timestamp <= '2026-06-30 00:00:00+00:00'" in sql
    assert "count(*) AS total" in sql
    assert "LEFT OUTER JOIN page_rows ON true" in sql
    assert "ORDER BY filtered_transactions.event_timestamp DESC" in sql
    assert "LIMIT 25 OFFSET 50" in sql
    session.execute.assert_awaited_once()


async def test_list_query_preserves_total_for_an_empty_page() -> None:
    session = AsyncMock(spec=AsyncSession)
    result = MagicMock()
    result.all.return_value = [(None, 3)]
    session.execute.return_value = result
    reader = SQLAlchemyTransactionReader(session)

    page = await reader.list_user_transactions(
        user_id=USER_ID,
        from_timestamp=None,
        to_timestamp=None,
        page=2,
        limit=100,
    )

    assert page.items == []
    assert page.total == 3
