from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from redis.exceptions import RedisError

from app.api.dependencies import get_metrics_store
from app.schemas.common import ErrorResponse
from app.services.metrics import MetricsStore, render_prometheus

router = APIRouter(tags=["Observability"])

METRICS_EXAMPLE = """# HELP events_processed_total Events successfully stored.
# TYPE events_processed_total counter
events_processed_total 12
# HELP events_failed_total Event processing attempts that failed.
# TYPE events_failed_total counter
events_failed_total 3
# HELP events_duplicate_total Duplicate events confirmed in PostgreSQL.
# TYPE events_duplicate_total counter
events_duplicate_total 4
"""


@router.get(
    "/metrics",
    summary="Get processing metrics",
    description=(
        "Return Redis-backed worker counters in Prometheus text exposition format. "
        "These counters are operational signals; PostgreSQL remains the source of "
        "truth for persisted transactions."
    ),
    response_model=None,
    response_class=Response,
    status_code=status.HTTP_200_OK,
    response_description="Prometheus text exposition containing worker counters.",
    responses={
        status.HTTP_200_OK: {
            "description": "Current processing counters.",
            "content": {
                "text/plain": {
                    "schema": {"type": "string"},
                    "example": METRICS_EXAMPLE,
                }
            },
        },
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "Redis metrics storage is unavailable.",
        },
    },
)
async def get_metrics(
    metrics: Annotated[MetricsStore, Depends(get_metrics_store)],
) -> Response:
    try:
        snapshot = await metrics.snapshot()
    except RedisError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "metrics_unavailable",
                "message": "Metrics are unavailable",
            },
        ) from error

    return Response(
        content=render_prometheus(snapshot),
        media_type="text/plain; version=0.0.4",
    )
