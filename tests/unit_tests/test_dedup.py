from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from app.currency import ConversionResult
from app.db.models import Transaction
from app.worker import ProcessingOutcome, QueuedEvent, TransactionEventProcessor


def make_event() -> QueuedEvent:
    return QueuedEvent(
        version=1,
        id=UUID("11111111-1111-1111-1111-111111111111"),
        user_id=UUID("22222222-2222-2222-2222-222222222222"),
        amount=Decimal("100.00"),
        currency="EUR",
        timestamp=datetime(2026, 6, 7, 12, tzinfo=UTC),
    )


class FakeSession:
    def __init__(
        self,
        existing_transaction: Transaction | None = None,
        inserted_id: UUID | None = None,
    ) -> None:
        self.existing_transaction = existing_transaction
        self.inserted_id = inserted_id
        self.get_calls = 0
        self.execute_calls = 0
        self.begin_calls = 0
        self.committed = False

    async def get(
        self,
        model: type[Transaction],
        object_id: UUID,
    ) -> Transaction | None:
        self.get_calls += 1
        return self.existing_transaction

    async def execute(self, statement: object) -> object:
        self.execute_calls += 1
        return FakeScalarResult(self.inserted_id)

    def begin(self) -> "FakeSession":
        self.begin_calls += 1
        return self

    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeScalarResult:
    def __init__(self, inserted_id: UUID | None) -> None:
        self.inserted_id = inserted_id

    def scalar_one_or_none(self) -> UUID | None:
        return self.inserted_id


class FakeSessionFactory:
    def __init__(self, sessions: list[FakeSession]) -> None:
        self._sessions = sessions

    def __call__(self) -> FakeSession:
        return self._sessions.pop(0)


class FakeConverter:
    def __init__(self) -> None:
        self.calls = 0

    async def convert_to_usd(self, amount: Decimal, currency: str) -> ConversionResult:
        self.calls += 1
        return ConversionResult(amount_usd=Decimal("108.00"), usd_rate=Decimal("1.08"))


async def test_dedup_short_circuits_before_conversion_and_insert() -> None:
    existing = Transaction(
        id=UUID("11111111-1111-1111-1111-111111111111"),
        user_id=UUID("22222222-2222-2222-2222-222222222222"),
        original_amount=Decimal("100.00"),
        original_currency="EUR",
        usd_rate=Decimal("1.0800000000"),
        amount_usd=Decimal("108.00"),
        event_timestamp=datetime(2026, 6, 7, 12, tzinfo=UTC),
    )
    session_factory = FakeSessionFactory([FakeSession(existing_transaction=existing)])
    converter = FakeConverter()
    processor = TransactionEventProcessor(session_factory, converter)  # type: ignore[arg-type]

    outcome = await processor.process(make_event())

    assert outcome is ProcessingOutcome.DUPLICATE
    assert converter.calls == 0


async def test_new_event_is_converted_and_inserted() -> None:
    session = FakeSession(inserted_id=UUID("11111111-1111-1111-1111-111111111111"))
    session_factory = FakeSessionFactory([session, session])
    converter = FakeConverter()
    processor = TransactionEventProcessor(session_factory, converter)  # type: ignore[arg-type]

    outcome = await processor.process(make_event())

    assert outcome is ProcessingOutcome.PROCESSED
    assert converter.calls == 1
    assert session.get_calls == 1
    assert session.execute_calls == 1
