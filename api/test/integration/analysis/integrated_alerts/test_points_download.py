import asyncio
import io
from test.integration.analysis.integrated_alerts.test_points import (
    TABLE_URI,
    VERSION,
    get_points,
)

import pandas as pd
import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from app.domain.analyzers.integrated_alerts_points_download_analyzer import (
    IntegratedAlertsPointsDownloadAnalyzer,
)
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.infrastructure.persistence.parquet_alert_points_repository import (
    ParquetAlertPointsRepository,
    build_points_input_uris,
)
from app.main import app
from app.models.land_change.integrated_alerts import POINTS_DOWNLOAD_ANALYTICS_NAME
from app.routers.land_change.integrated_alerts import integrated_alerts
from app.use_cases.analysis.analysis_service import AnalysisService


class InMemoryDownloadFileStore:
    def __init__(self):
        self.files = {}

    async def write_file(self, key, path, content_type):
        self.files[key] = path.read_bytes()

    async def presign_download_url(self, key, filename, expires_seconds):
        return key


def get_file_system_analysis_repository():
    return FileSystemAnalysisRepository(POINTS_DOWNLOAD_ANALYTICS_NAME)


async def download_points(request_body, file_store):
    def download_service() -> AnalysisService:
        return AnalysisService(
            analysis_repository=get_file_system_analysis_repository(),
            analyzer=IntegratedAlertsPointsDownloadAnalyzer(
                points_repository=ParquetAlertPointsRepository(TABLE_URI),
                file_store=file_store,
                input_uris=build_points_input_uris(VERSION),
            ),
            event=POINTS_DOWNLOAD_ANALYTICS_NAME,
        )

    app.dependency_overrides[
        integrated_alerts.create_points_download_analysis_service
    ] = download_service
    app.dependency_overrides[
        integrated_alerts.get_points_download_analysis_repository
    ] = get_file_system_analysis_repository
    app.dependency_overrides[integrated_alerts.get_download_file_store] = (
        lambda: file_store
    )
    app.dependency_overrides[integrated_alerts.get_latest_integrated_alerts_version] = (
        lambda: VERSION
    )
    try:
        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                created = await client.post(
                    "/v0/land_change/integrated_alerts/points/download",
                    json=request_body,
                )
                assert created.status_code == 202
                link = created.json()["data"]["link"]
                for _ in range(120):
                    data = (await client.get(link)).json()["data"]
                    if data["status"] != "pending":
                        return data
                    await asyncio.sleep(1)
                pytest.fail("Resource stuck on 'pending' status")
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_downloaded_csv_holds_the_same_rows_as_points():
    aoi = {"type": "admin", "ids": ["BRA.14.87"]}
    file_store = InMemoryDownloadFileStore()

    download = await download_points({"aoi": aoi, "format": "csv"}, file_store)
    points = await get_points({"aoi": aoi, "limit": 10_000})

    assert download["status"] == "saved"
    assert download["metadata"]["alerts"] == points["metadata"]["alerts"]
    csv = pd.read_csv(io.BytesIO(file_store.files[download["result"]["download_url"]]))
    expected = pd.DataFrame(points["result"])
    assert len(csv) == download["result"]["point_count"] > 0
    pd.testing.assert_frame_equal(csv.head(len(expected)), expected, check_dtype=False)
