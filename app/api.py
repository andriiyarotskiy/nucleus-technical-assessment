from collections.abc import AsyncIterator
from datetime import UTC
from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import AwareDatetime, BaseModel
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import AsyncSessionFactory
from app.events import (
    EventAcceptedResponse,
    EventProducer,
    EventPublishError,
    EventRequest,
)
from app.transactions import (
    SQLAlchemyTransactionReader,
    TransactionPageResponse,
    TransactionReader,
    TransactionResponse,
    UserSummaryResponse,
)

router = APIRouter()

MAX_PAGE = 1_000_000
MAX_PAGE_LIMIT = 100


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    detail: ErrorDetail


def get_event_producer(request: Request) -> EventProducer:
    return cast(EventProducer, request.app.state.event_producer)


async def get_db_session(request: Request) -> AsyncIterator[AsyncSession]:
    session_factory = cast(AsyncSessionFactory, request.app.state.session_factory)
    async with session_factory() as session:
        yield session


def get_transaction_reader(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TransactionReader:
    return SQLAlchemyTransactionReader(session)


def database_unavailable(error: SQLAlchemyError) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail={
            "code": "database_unavailable",
            "message": "Database is unavailable",
        },
    )


@router.post(
    "/events",
    response_model=EventAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "Redis Stream is unavailable",
        }
    },
)
async def publish_event(
    event: EventRequest,
    producer: Annotated[EventProducer, Depends(get_event_producer)],
) -> EventAcceptedResponse:
    try:
        await producer.publish(event)
    except EventPublishError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "event_publish_unavailable",
                "message": "Event queue is unavailable",
            },
        ) from error

    return EventAcceptedResponse(id=event.id)


@router.get(
    "/users/{user_id}/summary",
    response_model=UserSummaryResponse,
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "PostgreSQL is unavailable",
        }
    },
)
async def get_user_summary(
    user_id: UUID,
    reader: Annotated[TransactionReader, Depends(get_transaction_reader)],
) -> UserSummaryResponse:
    try:
        summary = await reader.get_user_summary(user_id)
    except SQLAlchemyError as error:
        raise database_unavailable(error) from error

    return UserSummaryResponse(
        user_id=user_id,
        total_usd=summary.total_usd,
        transaction_count=summary.transaction_count,
    )


@router.get(
    "/users/{user_id}/transactions",
    response_model=TransactionPageResponse,
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "PostgreSQL is unavailable",
        }
    },
)
async def list_user_transactions(
    user_id: UUID,
    reader: Annotated[TransactionReader, Depends(get_transaction_reader)],
    from_timestamp: Annotated[
        AwareDatetime | None,
        Query(alias="from"),
    ] = None,
    to_timestamp: Annotated[
        AwareDatetime | None,
        Query(alias="to"),
    ] = None,
    page: Annotated[int, Query(ge=1, le=MAX_PAGE)] = 1,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_LIMIT)] = 50,
) -> TransactionPageResponse:
    if (
        from_timestamp is not None
        and to_timestamp is not None
        and from_timestamp > to_timestamp
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="from must be earlier than or equal to to",
        )

    try:
        result = await reader.list_user_transactions(
            user_id=user_id,
            from_timestamp=(
                from_timestamp.astimezone(UTC) if from_timestamp is not None else None
            ),
            to_timestamp=(
                to_timestamp.astimezone(UTC) if to_timestamp is not None else None
            ),
            page=page,
            limit=limit,
        )
    except SQLAlchemyError as error:
        raise database_unavailable(error) from error

    return TransactionPageResponse(
        items=[
            TransactionResponse.model_validate(transaction)
            for transaction in result.items
        ],
        page=page,
        limit=limit,
        total=result.total,
        has_more=page * limit < result.total,
    )
