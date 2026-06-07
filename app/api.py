from typing import Annotated, cast

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from app.events import (
    EventAcceptedResponse,
    EventProducer,
    EventPublishError,
    EventRequest,
)

router = APIRouter()


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    detail: ErrorDetail


def get_event_producer(request: Request) -> EventProducer:
    return cast(EventProducer, request.app.state.event_producer)


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
