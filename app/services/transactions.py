from enum import StrEnum
from typing import Protocol
from uuid import UUID

from app.db.repositories.transactions import TransactionInsert
from app.schemas.events import QueuedEvent
from app.services.currency import CurrencyConverter


class ProcessingOutcome(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"


class TransactionStore(Protocol):
    async def exists(self, transaction_id: UUID) -> bool:
        """Return whether an event ID is already persisted."""

    async def insert_if_absent(self, record: TransactionInsert) -> bool:
        """Insert atomically and return whether a row was created."""


class EventProcessor(Protocol):
    async def process(self, event: QueuedEvent) -> ProcessingOutcome:
        """Persist one event and return whether it was newly inserted."""


class TransactionEventProcessor:
    def __init__(
        self,
        transactions: TransactionStore,
        converter: CurrencyConverter,
    ) -> None:
        self._transactions = transactions
        self._converter = converter

    async def process(self, event: QueuedEvent) -> ProcessingOutcome:
        if await self._transactions.exists(event.id):
            return ProcessingOutcome.DUPLICATE

        conversion = await self._converter.convert_to_usd(
            event.amount,
            event.currency,
        )
        inserted = await self._transactions.insert_if_absent(
            TransactionInsert(
                id=event.id,
                user_id=event.user_id,
                original_amount=event.amount,
                original_currency=event.currency,
                usd_rate=conversion.usd_rate,
                amount_usd=conversion.amount_usd,
                event_timestamp=event.timestamp,
            )
        )
        return ProcessingOutcome.PROCESSED if inserted else ProcessingOutcome.DUPLICATE
