from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from app.db.repositories.transactions import TransactionInsert
from app.schemas.events import QueuedEvent
from app.services.currency import ConversionResult
from app.services.transactions import ProcessingOutcome, TransactionEventProcessor


def make_event() -> QueuedEvent:
    return QueuedEvent(
        version=1,
        id=UUID("11111111-1111-1111-1111-111111111111"),
        user_id=UUID("22222222-2222-2222-2222-222222222222"),
        amount=Decimal("100.00"),
        currency="EUR",
        timestamp=datetime(2026, 6, 7, 12, tzinfo=UTC),
    )


class FakeTransactionStore:
    def __init__(self, *, exists: bool, inserted: bool = True) -> None:
        self._exists = exists
        self._inserted = inserted
        self.inserted_record: TransactionInsert | None = None

    async def exists(self, transaction_id: UUID) -> bool:
        return self._exists

    async def insert_if_absent(self, record: TransactionInsert) -> bool:
        self.inserted_record = record
        return self._inserted


class FakeConverter:
    def __init__(self) -> None:
        self.calls = 0

    async def convert_to_usd(self, amount: Decimal, currency: str) -> ConversionResult:
        self.calls += 1
        return ConversionResult(amount_usd=Decimal("108.00"), usd_rate=Decimal("1.08"))


async def test_existing_event_is_skipped_before_conversion() -> None:
    transactions = FakeTransactionStore(exists=True)
    converter = FakeConverter()
    processor = TransactionEventProcessor(
        transactions,
        converter,  # type: ignore[arg-type]
    )

    outcome = await processor.process(make_event())

    assert outcome is ProcessingOutcome.DUPLICATE
    assert converter.calls == 0
    assert transactions.inserted_record is None


@pytest.mark.parametrize(
    ("inserted", "expected_outcome"),
    [
        (True, ProcessingOutcome.PROCESSED),
        (False, ProcessingOutcome.DUPLICATE),
    ],
)
async def test_database_constraint_decides_final_dedup_outcome(
    inserted: bool,
    expected_outcome: ProcessingOutcome,
) -> None:
    transactions = FakeTransactionStore(exists=False, inserted=inserted)
    converter = FakeConverter()
    processor = TransactionEventProcessor(
        transactions,
        converter,  # type: ignore[arg-type]
    )

    outcome = await processor.process(make_event())

    assert outcome is expected_outcome
    assert converter.calls == 1
    assert transactions.inserted_record is not None
