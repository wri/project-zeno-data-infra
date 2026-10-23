import asyncio

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
import pytest_asyncio
import shapely
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from app.domain.analyzers.integrated_alerts_points_analyzer import (
    IntegratedAlertsPointsAnalyzer,
    build_points_input_uris,
)
from app.domain.repositories.gadm_aoi_geometry_repository import (
    GadmAoiGeometryRepository,
)
from app.infrastructure.external_services.alert_points_table import (
    day_to_date,
    load_alert_points_table,
)
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.main import app
from app.models.land_change.integrated_alerts import POINTS_ANALYTICS_NAME
from app.routers.land_change.integrated_alerts import integrated_alerts
from app.use_cases.analysis.analysis_service import AnalysisService

# Pin the integration test to a release with a published points table.
VERSION = "v20261018"
TABLE_URI = build_points_input_uris(VERSION)["alert_points_table_uri"]
CONFIDENCE = {2: "low", 3: "high", 4: "highest"}


def get_file_system_analysis_repository():
    return FileSystemAnalysisRepository(POINTS_ANALYTICS_NAME)


def create_points_analysis_service_for_tests() -> AnalysisService:
    return AnalysisService(
        analysis_repository=get_file_system_analysis_repository(),
        analyzer=IntegratedAlertsPointsAnalyzer(
            input_uris=build_points_input_uris(VERSION)
        ),
        event=POINTS_ANALYTICS_NAME,
    )


async def get_points(request_body):
    app.dependency_overrides[integrated_alerts.create_points_analysis_service] = (
        create_points_analysis_service_for_tests
    )
    app.dependency_overrides[integrated_alerts.get_points_analysis_repository] = (
        get_file_system_analysis_repository
    )
    app.dependency_overrides[integrated_alerts.get_points_request_version] = lambda: (
        VERSION
    )
    try:
        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                created = await client.post(
                    "/v0/land_change/integrated_alerts/points", json=request_body
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


def brute_force_rows(geom, start_date, end_date):
    """Every table point under geom in the window, read from every row group
    overlapping geom's row/col range (no polygon pruning or batching) and a
    whole-AOI point-in-polygon test, in the table's order: newest date first,
    then file order."""
    table = load_alert_points_table(TABLE_URI)
    min_x, min_y, max_x, max_y = geom.bounds
    cols = sorted([(min_x - table.x0) / table.dx, (max_x - table.x0) / table.dx])
    rows = sorted([(min_y - table.y0) / table.dy, (max_y - table.y0) / table.dy])
    groups = np.flatnonzero(
        (table.row_max >= int(np.floor(rows[0])))
        & (table.row_min <= int(np.ceil(rows[1])))
        & (table.col_max >= int(np.floor(cols[0])))
        & (table.col_min <= int(np.ceil(cols[1])))
    )
    with table.filesystem.open_input_file(table.path) as f:
        points = pq.ParquetFile(f).read_row_groups(groups.tolist()).to_pandas()
    dates = points.alert_date.map(lambda d: day_to_date(d).isoformat())
    keep = (dates >= start_date) & (dates <= end_date)
    keep &= shapely.contains_xy(
        geom, table.lon(points.col.to_numpy()), table.lat(points.row.to_numpy())
    )
    points = points[keep.to_numpy()].sort_values(
        "alert_date", ascending=False, kind="stable", ignore_index=True
    )
    return pd.DataFrame(
        {
            "lat": table.lat(points.row.to_numpy()),
            "lon": table.lon(points.col.to_numpy()),
            "alert_date": points.alert_date.map(lambda d: day_to_date(d).isoformat()),
            "alert_confidence": points.confidence.map(CONFIDENCE),
            "natural_lands_class": points.natural_lands_class.astype(int),
        }
    )


@pytest_asyncio.fixture
async def admin_points():
    return await get_points(
        {"aoi": {"type": "admin", "ids": ["BRA.14.87"]}, "limit": 10_000}
    )


@pytest.mark.asyncio
async def test_admin_points_match_brute_force(admin_points):
    assert admin_points["status"] == "saved"
    alerts = admin_points["metadata"]["alerts"]
    assert alerts["version"] == VERSION
    geometries, _ = await GadmAoiGeometryRepository().load("admin", ["BRA.14.87"])
    expected = brute_force_rows(
        geometries[0], alerts["start_date"], alerts["end_date"]
    ).head(10_000)
    result = pd.DataFrame(admin_points["result"])
    assert len(result) > 0
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
