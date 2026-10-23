import asyncio
from test.integration import delete_resource_files
from test.unit.alert_points import aoi_polygon, make_points, write_points_table

import pytest
from fastapi.testclient import TestClient

from app.domain.analyzers.integrated_alerts_points_analyzer import (
    IntegratedAlertsPointsAnalyzer,
)
from app.domain.models.environment import Environment
from app.infrastructure.external_services.alert_points_table import (
    load_alert_points_table,
)
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.main import app
from app.models.land_change.integrated_alerts import (
    POINTS_ANALYTICS_NAME,
    IntegratedAlertsPointsAnalyticsIn,
    PointsCursor,
    encode_cursor,
)
from app.routers.land_change.integrated_alerts import integrated_alerts
from app.routers.land_change.integrated_alerts.integrated_alerts import (
    get_latest_alert_date,
    get_latest_integrated_alerts_version,
)
from app.use_cases.analysis.analysis_service import AnalysisService
from app.use_cases.integrated_alerts.latest_alert_date import LatestAlertDateNotFound

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


def test_latest_returns_503_and_retries_when_no_alert_date_found(monkeypatch):
    queried = []

    class FakeMissingLatestDate:
        def __init__(self, duckdb_query_service):
            pass

        async def get(self) -> str:
            queried.append(True)
            raise LatestAlertDateNotFound()

    monkeypatch.setattr(
        integrated_alerts, "IntegratedAlertsLatestDate", FakeMissingLatestDate
    )
    app.dependency_overrides[get_latest_integrated_alerts_version] = lambda: "v1"
    get_latest_alert_date.cache_clear()
    try:
        responses = [
            client.get("/v0/land_change/integrated_alerts/latest") for _ in range(2)
        ]
    finally:
        app.dependency_overrides.clear()
        get_latest_alert_date.cache_clear()

    assert [response.status_code for response in responses] == [503, 503]
    assert len(queried) == 2


class FakeRequest:
    def __init__(self, body):
        self.body = body

    async def json(self):
        if self.body is None:
            raise ValueError("no JSON body")
        return self.body


def _request_version(body, latest="v20261020"):
    return asyncio.run(
        integrated_alerts.get_points_request_version(FakeRequest(body), latest)
    )


def test_points_request_without_a_cursor_uses_the_latest_version():
    assert _request_version({"limit": 5}) == "v20261020"
    assert _request_version(None) == "v20261020"


def test_points_request_with_a_cursor_uses_the_cursors_version():
    cursor = encode_cursor(PointsCursor("v20261018", 3, 4, "list"))
    assert _request_version({"cursor": cursor}) == "v20261018"


def test_points_request_with_a_malformed_cursor_falls_back_to_latest():
    assert _request_version({"cursor": "nope"}) == "v20261020"


def test_points_request_with_a_malformed_cursor_gets_422():
    app.dependency_overrides[integrated_alerts.get_points_request_version] = lambda: (
        "v20261018"
    )
    app.dependency_overrides[integrated_alerts.get_points_analysis_repository] = (
        lambda: None
    )
    app.dependency_overrides[integrated_alerts.create_points_analysis_service] = (
        lambda: None
    )
    try:
        response = client.post(
            "/v0/land_change/integrated_alerts/points",
            json={"aoi": {"type": "admin", "ids": ["BRA.14.87"]}, "cursor": "nope"},
        )
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 422


def test_points_request_with_another_lists_cursor_gets_422():
    first_page = {"aoi": {"type": "admin", "ids": ["BRA.14.87"]}}
    list_id = IntegratedAlertsPointsAnalyticsIn(**first_page).list_id()
    cursor = encode_cursor(PointsCursor("v20261018", 3, 4, list_id))
    app.dependency_overrides[integrated_alerts.get_points_request_version] = lambda: (
        "v20261018"
    )
    app.dependency_overrides[integrated_alerts.get_points_analysis_repository] = (
        lambda: None
    )
    app.dependency_overrides[integrated_alerts.create_points_analysis_service] = (
        lambda: None
    )
    try:
        response = client.post(
            "/v0/land_change/integrated_alerts/points",
            json={"aoi": {"type": "admin", "ids": ["BRA.14"]}, "cursor": cursor},
        )
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 422
    assert "cursor is for a different list" in response.text


def test_points_post_then_get_returns_points(tmp_path):
    load_alert_points_table.cache_clear()
    table_uri = write_points_table(tmp_path / "by-date.parquet", make_points())
    repository = FileSystemAnalysisRepository(POINTS_ANALYTICS_NAME)

    class FakeGeometryRepository:
        async def load(self, aoi_type, aoi_ids):
            return [aoi_polygon()] * len(aoi_ids), [0] * len(aoi_ids)

    def points_service():
        return AnalysisService(
            analysis_repository=repository,
            analyzer=IntegratedAlertsPointsAnalyzer(
                input_uris={"alert_points_table_uri": table_uri},
                geometry_repository=FakeGeometryRepository(),
            ),
            event=POINTS_ANALYTICS_NAME,
        )

    request = {"aoi": {"type": "admin", "ids": ["BRA.14.87"]}, "limit": 3}
    app.dependency_overrides[integrated_alerts.get_points_request_version] = lambda: (
        "v20261018"
    )
    app.dependency_overrides[integrated_alerts.create_points_analysis_service] = (
        points_service
    )
    app.dependency_overrides[integrated_alerts.get_points_analysis_repository] = (
        lambda: repository
    )
    try:
        created = client.post("/v0/land_change/integrated_alerts/points", json=request)
        link = created.json()["data"]["link"]
        resource_id = link.rsplit("/", 1)[1]
        result = client.get(f"/v0/land_change/integrated_alerts/points/{resource_id}")
    finally:
        app.dependency_overrides.clear()
        load_alert_points_table.cache_clear()
        delete_resource_files(POINTS_ANALYTICS_NAME, resource_id)

    assert created.status_code == 202
    assert link.startswith("http://testserver/v0/land_change/integrated_alerts/points/")
    data = result.json()["data"]
    assert data["status"] == "saved"
    assert set(data["result"]) == {
        "lat",
        "lon",
        "alert_date",
        "alert_confidence",
        "natural_lands_class",
    }
    assert data["result"]["alert_date"] == ["2026-10-16"] * 3
    assert data["metadata"]["alerts"]["version"] == "v20261018"
    assert data["metadata"]["truncated"] == {"BRA.14.87": True}
    assert data["metadata"]["limit"] == 3
    assert data["metadata"]["next_cursor"] is not None
