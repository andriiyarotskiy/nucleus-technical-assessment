from datetime import UTC
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import AwareDatetime
from sqlalchemy.exc import SQLAlchemyError

from app.api.dependencies import get_transaction_repository
from app.db.repositories.transactions import TransactionRepository
from app.schemas.common import ErrorResponse
from app.schemas.transactions import (
    TransactionPageResponse,
    TransactionResponse,
    UserSummaryResponse,
)

router = APIRouter(tags=["Users"])

MAX_PAGE = 1_000_000
MAX_PAGE_LIMIT = 100


def database_unavailable() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail={
            "code": "database_unavailable",
            "message": "Database is unavailable",
        },
    )


@router.get(
    "/users/{user_id}/summary",
    summary="Get a user's transaction summary",
    description=(
        "Aggregate all persisted transactions for one user. Users without "
        "transactions receive a successful response with zero totals."
    ),
    response_model=UserSummaryResponse,
    status_code=status.HTTP_200_OK,
    response_description="The user's persisted USD total and transaction count.",
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "PostgreSQL is unavailable.",
        }
    },
)
async def get_user_summary(
    user_id: Annotated[
        UUID,
        Path(
            description="User identifier to aggregate.",
            examples=["22222222-2222-2222-2222-222222222222"],
        ),
    ],
    transactions: Annotated[
        TransactionRepository,
        Depends(get_transaction_repository),
    ],
) -> UserSummaryResponse:
    try:
        summary = await transactions.get_user_summary(user_id)
    except SQLAlchemyError as error:
        raise database_unavailable() from error

    return UserSummaryResponse(
        user_id=user_id,
        total_usd=summary.total_usd,
        transaction_count=summary.transaction_count,
    )


@router.get(
    "/users/{user_id}/transactions",
    summary="List a user's transactions",
    description=(
        "Return a page of persisted transactions ordered by event timestamp "
        "descending. Optional timezone-aware bounds are inclusive."
    ),
    response_model=TransactionPageResponse,
    status_code=status.HTTP_200_OK,
    response_description="A page of matching transactions.",
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "PostgreSQL is unavailable.",
        }
    },
)
async def list_user_transactions(
    user_id: Annotated[
        UUID,
        Path(
            description="User identifier whose transactions are requested.",
            examples=["22222222-2222-2222-2222-222222222222"],
        ),
    ],
    transactions: Annotated[
        TransactionRepository,
        Depends(get_transaction_repository),
    ],
    from_timestamp: Annotated[
        AwareDatetime | None,
        Query(
            alias="from",
            description="Inclusive timezone-aware lower timestamp bound.",
            examples=["2026-06-01T00:00:00Z"],
        ),
    ] = None,
    to_timestamp: Annotated[
        AwareDatetime | None,
        Query(
            alias="to",
            description="Inclusive timezone-aware upper timestamp bound.",
            examples=["2026-06-30T23:59:59Z"],
        ),
    ] = None,
    page: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_PAGE,
            description="One-based page number.",
            examples=[1],
        ),
    ] = 1,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_PAGE_LIMIT,
            description="Number of transactions per page, capped at 100.",
            examples=[50],
        ),
    ] = 50,
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
        result = await transactions.list_user_transactions(
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
        raise database_unavailable() from error

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
