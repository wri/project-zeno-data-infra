import io
import zipfile
from test.unit.alert_points import (
    DAYS_STORED,
    LATEST_DAY,
    aoi_polygon,
    brute_force,
    lat,
    lon,
    make_points,
    write_points_table,
)

import geopandas as gpd
import pandas as pd
import pytest

from app.domain.analyzers import integrated_alerts_points_download_analyzer
from app.domain.analyzers.integrated_alerts_points_download_analyzer import (
    IntegratedAlertsPointsDownloadAnalyzer,
)
from app.domain.models.analysis import Analysis
from app.infrastructure.persistence.parquet_alert_points_repository import (
    ParquetAlertPointsRepository,
    day_to_date,
    load_alert_points_table,
)
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.integrated_alerts import IntegratedAlertsPointsDownloadIn

FIRST_DAY = LATEST_DAY - DAYS_STORED + 1


class FakeGeometryRepository:
    async def load(self, aoi_type, aoi_ids):
        return [aoi_polygon()] * len(aoi_ids), [0] * len(aoi_ids)


class InMemoryFileStore:
    def __init__(self):
        self.files = {}

    async def write_file(self, key, path, content_type):
        self.files[key] = (path.read_bytes(), content_type)


@pytest.fixture
def points():
    return make_points()


@pytest.fixture
def table_uri(tmp_path, points):
    load_alert_points_table.cache_clear()
    yield write_points_table(tmp_path / "by-date.parquet", points)
    load_alert_points_table.cache_clear()


async def _download(table_uri, **request):
    file_store = InMemoryFileStore()
    analyzer = IntegratedAlertsPointsDownloadAnalyzer(
        points_repository=ParquetAlertPointsRepository(table_uri),
        file_store=file_store,
        geometry_repository=FakeGeometryRepository(),
    )
    analytics_in = IntegratedAlertsPointsDownloadIn(
        aoi=AdminAreaOfInterest(ids=["BRA.14.87"]), **request
    )
    analysis = Analysis(metadata=analytics_in.model_dump(), result=None, status=None)
    await analyzer.analyze(analysis)
    content, content_type = file_store.files[analysis.result["file_key"]]
    return analysis, content, content_type


def _expected_rows(points, start_day, end_day, confidences, classes):
    expected = brute_force(
        points, aoi_polygon(), start_day, end_day, confidences, classes
    )
    return pd.DataFrame(
        {
            "lat": lat(expected.row),
            "lon": lon(expected.col),
            "alert_date": [day_to_date(d).isoformat() for d in expected.alert_date],
            "alert_confidence": expected.confidence.map(
                {2: "low", 3: "high", 4: "highest"}
            ),
            "natural_lands_class": expected.natural_lands_class.astype(int),
        }
    )


@pytest.mark.asyncio
async def test_download_csv_has_one_row_per_alert_with_points_columns(
    table_uri, points
):
    analysis, content, content_type = await _download(
        table_uri,
        format="csv",
        alert_confidence=["high", "highest"],
        land_filter="natural_lands",
    )

    csv = pd.read_csv(io.BytesIO(content))
    expected = _expected_rows(points, FIRST_DAY, LATEST_DAY, [3, 4], range(2, 12))
    assert len(csv) > 0
    pd.testing.assert_frame_equal(csv, expected, check_dtype=False)
    assert content_type == "text/csv"
    assert analysis.result["format"] == "csv"
    assert analysis.result["point_count"] == len(expected)
    assert analysis.result["filename"] == (
        f"integrated_alerts_BRA.14.87_{day_to_date(FIRST_DAY)}_2026-10-16.csv"
    )
    assert analysis.result["file_key"].startswith(
        "integrated_alerts_points_download/v20261018/"
    )
    assert analysis.metadata["truncated"] == {"BRA.14.87": False}


@pytest.mark.asyncio
async def test_download_shapefile_zip_holds_point_layer_with_short_column_names(
    table_uri, points
):
    analysis, content, content_type = await _download(table_uri, format="shp")

    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = archive.namelist()
    layer = gpd.read_file(io.BytesIO(content))
    expected = _expected_rows(points, FIRST_DAY, LATEST_DAY, [2, 3, 4], None)
    assert content_type == "application/zip"
    assert {name.rsplit(".", 1)[1] for name in names} >= {"shp", "shx", "dbf", "prj"}
    assert list(layer.columns) == [
        "lat",
        "lon",
        "alert_date",
        "confidence",
        "nl_class",
        "geometry",
    ]
    assert layer.crs.to_epsg() == 4326
    assert len(layer) == len(expected) == analysis.result["point_count"]
    assert list(layer.geometry.x) == pytest.approx(list(expected.lon))
    assert list(layer.geometry.y) == pytest.approx(list(expected.lat))
    assert list(layer.confidence) == list(expected.alert_confidence)


@pytest.mark.asyncio
async def test_download_is_truncated_past_max_points(table_uri, points, monkeypatch):
    monkeypatch.setattr(
        integrated_alerts_points_download_analyzer, "POINTS_DOWNLOAD_MAX_POINTS", 25
    )

    analysis, content, _ = await _download(table_uri, format="csv")

    csv = pd.read_csv(io.BytesIO(content))
    expected = _expected_rows(points, FIRST_DAY, LATEST_DAY, [2, 3, 4], None)
    pd.testing.assert_frame_equal(csv, expected.head(25), check_dtype=False)
    assert analysis.metadata["truncated"] == {"BRA.14.87": True}


@pytest.mark.asyncio
async def test_download_dates_are_clipped_to_stored_window(table_uri, points):
    analysis, content, _ = await _download(
        table_uri,
        format="csv",
        start_date=day_to_date(LATEST_DAY - 1).isoformat(),
        end_date="2027-01-01",
    )

    csv = pd.read_csv(io.BytesIO(content))
    expected = _expected_rows(points, LATEST_DAY - 1, LATEST_DAY, [2, 3, 4], None)
    pd.testing.assert_frame_equal(csv, expected, check_dtype=False)
    assert analysis.metadata["alerts"]["start_date"] == str(day_to_date(LATEST_DAY - 1))
    assert analysis.metadata["alerts"]["end_date"] == "2026-10-16"


@pytest.mark.asyncio
@pytest.mark.parametrize("download_format", ["csv", "shp"])
async def test_download_outside_stored_window_writes_an_empty_file(
    table_uri, download_format
):
    analysis, content, _ = await _download(
        table_uri,
        format=download_format,
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert analysis.result["point_count"] == 0
    assert analysis.result["filename"].startswith("integrated_alerts_BRA.14.87_none_")
    assert len(content) > 0
