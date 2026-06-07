from httpx import ASGITransport, AsyncClient

from app.api import get_metrics_store
from app.main import create_app
from app.metrics import MetricsSnapshot, render_prometheus


class FixedMetricsStore:
    async def increment(self, metric_name: str) -> None:
        raise NotImplementedError

    async def snapshot(self) -> MetricsSnapshot:
        return MetricsSnapshot(
            events_processed_total=12,
            events_failed_total=3,
            events_duplicate_total=4,
        )


def test_prometheus_renderer_includes_required_counters() -> None:
    text = render_prometheus(
        MetricsSnapshot(
            events_processed_total=0,
            events_failed_total=0,
            events_duplicate_total=0,
        )
    )

    assert "events_processed_total 0" in text
    assert "events_failed_total 0" in text
    assert "events_duplicate_total 0" in text


async def test_metrics_endpoint_returns_prometheus_text() -> None:
    app = create_app()
    app.dependency_overrides[get_metrics_store] = FixedMetricsStore

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain; version=0.0.4")
    assert "events_processed_total 12" in response.text
    assert "events_failed_total 3" in response.text
    assert "events_duplicate_total 4" in response.text
