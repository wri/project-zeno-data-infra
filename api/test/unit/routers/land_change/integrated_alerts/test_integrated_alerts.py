import pytest
from fastapi.testclient import TestClient

from app.domain.models.environment import Environment
from app.main import app
from app.routers.land_change.integrated_alerts import integrated_alerts
from app.routers.land_change.integrated_alerts.integrated_alerts import (
    get_latest_alert_date,
    get_latest_integrated_alerts_version,
)

client = TestClient(app)


def test_latest_returns_alert_date_and_version():
    app.dependency_overrides[get_latest_integrated_alerts_version] = lambda: (
        "v20261014"
    )
    app.dependency_overrides[get_latest_alert_date] = lambda: "2026-10-12"
    try:
        response = client.get("/v0/land_change/integrated_alerts/latest")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "data": {"alert_date": "2026-10-12", "version": "v20261014"},
        "status": "success",
    }


@pytest.mark.asyncio
async def test_latest_alert_date_is_queried_once_per_version(monkeypatch):
    queried = []

    class FakeLatestDate:
        def __init__(self, duckdb_query_service):
            self.table_uri = duckdb_query_service.table_uri

        async def get(self) -> str:
            queried.append(self.table_uri)
            return "2026-10-12"

    monkeypatch.setattr(integrated_alerts, "IntegratedAlertsLatestDate", FakeLatestDate)
    get_latest_alert_date.cache_clear()

    for version in ["v1", "v1", "v2"]:
        assert (
            await get_latest_alert_date(version, Environment.production) == "2026-10-12"
        )

    assert len(queried) == 2
    get_latest_alert_date.cache_clear()
