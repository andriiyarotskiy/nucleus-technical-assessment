from typing import cast

from fastapi import Request

from app.db.repositories.transactions import TransactionRepository
from app.services.events import EventProducer
from app.services.metrics import MetricsStore


def get_event_producer(request: Request) -> EventProducer:
    return cast(EventProducer, request.app.state.event_producer)


def get_metrics_store(request: Request) -> MetricsStore:
    return cast(MetricsStore, request.app.state.metrics_store)


def get_transaction_repository(request: Request) -> TransactionRepository:
    return cast(TransactionRepository, request.app.state.transaction_repository)
