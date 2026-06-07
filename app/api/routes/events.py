from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.dependencies import get_event_producer
from app.schemas.common import ErrorResponse
from app.schemas.events import EventAcceptedResponse, EventRequest
from app.services.events import EventProducer, EventPublishError

router = APIRouter(tags=["Events"])


@router.post(
    "/events",
    summary="Accept a transaction event",
    description=(
        "Validate a transaction event and append it to Redis Streams for "
        "asynchronous processing. A 202 response confirms queue acceptance only; "
        "the worker persists the transaction later."
    ),
    response_model=EventAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    response_description="The event was appended to Redis Streams.",
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "Redis Streams could not accept the event.",
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
