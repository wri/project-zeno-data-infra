from datetime import date
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

import pandas as pd
import pytest
from shapely.geometry import mapping

from app.domain.analyzers.integrated_alerts_points_analyzer import (
    IntegratedAlertsPointsAnalyzer,
    served_dates,
)
from app.domain.models.analysis import Analysis
from app.domain.repositories.alert_points_repository import (
    AlertPointsPage,
    AlertPointsQuery,
    AlertPointsRepository,
    AlertPointsSource,
)
from app.infrastructure.persistence.parquet_alert_points_repository import (
    ParquetAlertPointsRepository,
    day_to_date,
    load_alert_points_table,
)
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
)
from app.models.land_change.integrated_alerts import (
    IntegratedAlertsPointsAnalyticsIn,
    PointsCursor,
    decode_cursor,
    encode_cursor,
)

FIRST_DAY = LATEST_DAY - DAYS_STORED + 1


class FakeGeometryRepository:
    def __init__(self, geometries):
        self.geometries = geometries
        self.requests = []

    async def load(self, aoi_type, aoi_ids):
        self.requests.append((aoi_type, list(aoi_ids)))
        return [self.geometries[i] for i in aoi_ids], [0] * len(aoi_ids)


@pytest.fixture
def points():
    return make_points()


@pytest.fixture
def table_uri(tmp_path, points):
    load_alert_points_table.cache_clear()
    yield write_points_table(tmp_path / "by-date.parquet", points)
    load_alert_points_table.cache_clear()


def _analyzer(table_uri, geometries):
    return IntegratedAlertsPointsAnalyzer(
        points_repository=ParquetAlertPointsRepository(table_uri),
        geometry_repository=FakeGeometryRepository(geometries),
    )


def _analysis(analytics_in):
    return Analysis(metadata=analytics_in.model_dump(), result=None, status=None)


def _expected_rows(points, geom, start_day, end_day, confidences, classes):
    expected = brute_force(points, geom, start_day, end_day, confidences, classes)
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
async def test_admin_points_with_filters_and_metadata(table_uri, points):
    geom = aoi_polygon()
    analytics_in = IntegratedAlertsPointsAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA.14.87"]),
        start_date=day_to_date(LATEST_DAY - 2).isoformat(),
        alert_confidence=["high", "highest"],
        land_filter="natural_lands",
        limit=10_000,
    )
    analysis = _analysis(analytics_in)

    await _analyzer(table_uri, {"BRA.14.87": geom}).analyze(analysis)

    result = pd.DataFrame(analysis.result)
    expected = _expected_rows(
        points, geom, LATEST_DAY - 2, LATEST_DAY, [3, 4], range(2, 12)
    )
    assert len(result) > 0
    assert list(result.columns) == [
        "lat",
        "lon",
        "alert_date",
        "alert_confidence",
        "natural_lands_class",
    ]
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
    assert analysis.metadata["alerts"] == {
        "version": "v20261018",
        "latest_alert_date": "2026-10-16",
        "stored_start_date": day_to_date(FIRST_DAY).isoformat(),
        "stored_end_date": "2026-10-16",
        "start_date": day_to_date(LATEST_DAY - 2).isoformat(),
        "end_date": "2026-10-16",
    }
    assert analysis.metadata["truncated"] == {"BRA.14.87": False}


@pytest.mark.asyncio
async def test_limit_returns_the_newest_points_and_marks_truncation(table_uri, points):
    geom = aoi_polygon()
    analytics_in = IntegratedAlertsPointsAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA.1"]), limit=25
    )
    analysis = _analysis(analytics_in)

    await _analyzer(table_uri, {"BRA.1": geom}).analyze(analysis)

    result = pd.DataFrame(analysis.result)
    expected = _expected_rows(
        points, geom, FIRST_DAY, LATEST_DAY, [2, 3, 4], None
    ).head(25)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
    assert analysis.metadata["truncated"] == {"BRA.1": True}


@pytest.mark.asyncio
async def test_dates_outside_the_stored_window_return_no_points(table_uri):
    analytics_in = IntegratedAlertsPointsAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA.1"]),
        start_date="2024-01-01",
        end_date="2024-12-31",
    )
    analysis = _analysis(analytics_in)

    await _analyzer(table_uri, {"BRA.1": aoi_polygon()}).analyze(analysis)

    assert analysis.result["lat"] == []
    assert analysis.metadata["alerts"]["start_date"] is None
    assert analysis.metadata["alerts"]["end_date"] is None
    assert analysis.metadata["truncated"] == {"BRA.1": False}


@pytest.mark.asyncio
async def test_aoi_without_geometry_fails(table_uri):
    class MissingGeometryRepository:
        async def load(self, aoi_type, aoi_ids):
            return [], []

    analyzer = IntegratedAlertsPointsAnalyzer(
        points_repository=ParquetAlertPointsRepository(table_uri),
        geometry_repository=MissingGeometryRepository(),
    )
    analytics_in = IntegratedAlertsPointsAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA.99"])
    )

    with pytest.raises(ValueError, match="Expected 1 geometries"):
        await analyzer.analyze(_analysis(analytics_in))


@pytest.mark.parametrize(
    "start_date, end_date, expected",
    [
        (None, None, (FIRST_DAY, LATEST_DAY)),
        ("2026-01-01", "2027-01-01", (FIRST_DAY, LATEST_DAY)),
        ("2026-10-15", "2026-10-15", (LATEST_DAY - 1, LATEST_DAY - 1)),
    ],
)
def test_served_dates_clip_the_request_to_the_stored_window(
    start_date, end_date, expected
):
    source = AlertPointsSource(
        version="v20261018",
        first_date=day_to_date(FIRST_DAY),
        latest_date=day_to_date(LATEST_DAY),
    )
    analytics_in = IntegratedAlertsPointsAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA"]), start_date=start_date, end_date=end_date
    )
    assert served_dates(source, analytics_in) == tuple(day_to_date(d) for d in expected)


def test_points_request_validation():
    with pytest.raises(ValueError):
        IntegratedAlertsPointsAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA"]), limit=10_001
        )
    with pytest.raises(ValueError):
        IntegratedAlertsPointsAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA"]), alert_confidence=[]
        )
    with pytest.raises(ValueError):
        IntegratedAlertsPointsAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA"]), alert_confidence=["medium"]
        )


@pytest.mark.parametrize(
    "aoi",
    [
        {"type": "protected_area", "ids": ["555758964"]},
        {"type": "key_biodiversity_area", "ids": ["1"]},
        {"type": "indigenous_land", "ids": ["MEX11287"]},
        {
            "type": "feature_collection",
            "feature_collection": {
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "id": "a", "geometry": mapping(aoi_polygon())}
                ],
            },
        },
    ],
)
def test_points_request_takes_only_gadm_aois(aoi):
    with pytest.raises(ValueError):
        IntegratedAlertsPointsAnalyticsIn(aoi=aoi)


def test_points_request_takes_exactly_one_aoi():
    with pytest.raises(ValueError, match="1 AOI per request"):
        IntegratedAlertsPointsAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.1", "BRA.2"])
        )


@pytest.mark.asyncio
async def test_cursors_page_through_the_whole_list(table_uri, points):
    geom = aoi_polygon()
    analyzer = _analyzer(table_uri, {"BRA.1": geom})
    rows, cursor, pages = [], None, 0
    while True:
        analytics_in = IntegratedAlertsPointsAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.1"]), limit=150, cursor=cursor
        )
        analysis = _analysis(analytics_in)
        await analyzer.analyze(analysis)
        rows.append(pd.DataFrame(analysis.result))
        pages += 1
        cursor = analysis.metadata["next_cursor"]
        assert (cursor is None) == (not analysis.metadata["truncated"]["BRA.1"])
        if cursor is None:
            break

    expected = _expected_rows(points, geom, FIRST_DAY, LATEST_DAY, [2, 3, 4], None)
    assert pages > 2
    pd.testing.assert_frame_equal(
        pd.concat(rows, ignore_index=True), expected, check_dtype=False
    )


@pytest.mark.asyncio
async def test_cursor_from_another_version_is_refused(table_uri):
    aoi = AdminAreaOfInterest(ids=["BRA.1"])
    list_id = IntegratedAlertsPointsAnalyticsIn(aoi=aoi).list_id()
    analytics_in = IntegratedAlertsPointsAnalyticsIn(
        aoi=aoi,
        cursor=encode_cursor(PointsCursor("v20261001", "3:4", list_id)),
    )

    with pytest.raises(ValueError, match="cursor is for alerts version v20261001"):
        await _analyzer(table_uri, {"BRA.1": aoi_polygon()}).analyze(
            _analysis(analytics_in)
        )


def test_cursor_round_trips_and_rejects_garbage():
    cursor = PointsCursor("v20261018", "12:7", "0123456789abcdef")
    assert decode_cursor(encode_cursor(cursor)) == cursor
    for bad in ["", "not-a-cursor", encode_cursor(PointsCursor("v1", "", "x"))]:
        with pytest.raises(ValueError, match="Not a valid points cursor"):
            decode_cursor(bad)
    with pytest.raises(ValueError, match="Not a valid points cursor"):
        IntegratedAlertsPointsAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.1"]), cursor="not-a-cursor"
        )


@pytest.mark.parametrize(
    "changed",
    [
        {"aoi": AdminAreaOfInterest(ids=["BRA.2"])},
        {"start_date": "2026-10-14"},
        {"end_date": "2026-10-15"},
        {"alert_confidence": ["highest"]},
        {"land_filter": "natural_forests"},
    ],
)
def test_cursor_from_a_different_list_is_rejected(changed):
    first_page = dict(
        aoi=AdminAreaOfInterest(ids=["BRA.1"]),
        start_date="2026-10-12",
        end_date="2026-10-16",
        alert_confidence=["high", "highest"],
        land_filter="natural_lands",
    )
    list_id = IntegratedAlertsPointsAnalyticsIn(**first_page).list_id()
    cursor = encode_cursor(PointsCursor("v20261018", "3:4", list_id))

    IntegratedAlertsPointsAnalyticsIn(**first_page, cursor=cursor, limit=50)
    with pytest.raises(ValueError, match="cursor is for a different list"):
        IntegratedAlertsPointsAnalyticsIn(**{**first_page, **changed}, cursor=cursor)


def test_list_id_ignores_how_dates_and_confidences_are_written():
    aoi = AdminAreaOfInterest(ids=["BRA.1"])
    a = IntegratedAlertsPointsAnalyticsIn(
        aoi=aoi, start_date="2026-10-01", alert_confidence=["low", "high"]
    )
    b = IntegratedAlertsPointsAnalyticsIn(
        aoi=aoi, start_date="2026-10-1", alert_confidence=["high", "low"], limit=7
    )
    assert a.list_id() == b.list_id()


def test_end_date_before_start_date_is_rejected():
    aoi = AdminAreaOfInterest(ids=["BRA.1"])
    with pytest.raises(ValueError, match="end_date must be on or after start_date"):
        IntegratedAlertsPointsAnalyticsIn(
            aoi=aoi, start_date="2026-10-15", end_date="2026-10-1"
        )
    IntegratedAlertsPointsAnalyticsIn(
        aoi=aoi, start_date="2026-10-15", end_date="2026-10-15"
    )
    IntegratedAlertsPointsAnalyticsIn(aoi=aoi, start_date="2026-10-15")


class InMemoryAlertPointsRepository(AlertPointsRepository):
    """Serves a fixed list of points in pages, with positions as list offsets,
    to check the analyzer depends only on the AlertPointsRepository interface."""

    def __init__(self, points: pd.DataFrame):
        self.points = points
        self.queries: list[AlertPointsQuery] = []

    async def source(self) -> AlertPointsSource:
        return AlertPointsSource("v20261020", date(2026, 10, 1), date(2026, 10, 18))

    async def read_most_recent_points(self, query) -> AlertPointsPage:
        self.queries.append(query)
        start = 0 if query.after is None else int(query.after) + 1
        stop = start + query.limit
        page = self.points.iloc[start:stop]
        more = stop < len(self.points)
        return AlertPointsPage(
            points=page.reset_index(drop=True),
            more=more,
            last_position=str(start + len(page) - 1) if len(page) else None,
        )


@pytest.mark.asyncio
async def test_analyzer_pages_through_any_points_repository():
    points = pd.DataFrame(
        {
            "lat": [-3.1, -3.2, -3.3],
            "lon": [-52.1, -52.2, -52.3],
            "alert_date": [date(2026, 10, 18), date(2026, 10, 17), date(2026, 10, 2)],
            "confidence": [4, 3, 3],
            "natural_lands_class": [2, 0, 5],
        }
    )
    repository = InMemoryAlertPointsRepository(points)
    analyzer = IntegratedAlertsPointsAnalyzer(
        points_repository=repository,
        geometry_repository=FakeGeometryRepository({"BRA.1": aoi_polygon()}),
    )

    pages, cursor = [], None
    while True:
        analytics_in = IntegratedAlertsPointsAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.1"]),
            start_date="2026-09-01",
            alert_confidence=["high", "highest"],
            land_filter="natural_forests",
            limit=2,
            cursor=cursor,
        )
        analysis = _analysis(analytics_in)
        await analyzer.analyze(analysis)
        pages.append(analysis.result)
        cursor = analysis.metadata["next_cursor"]
        if cursor is None:
            break

    assert pages == [
        {
            "lat": [-3.1, -3.2],
            "lon": [-52.1, -52.2],
            "alert_date": ["2026-10-18", "2026-10-17"],
            "alert_confidence": ["highest", "high"],
            "natural_lands_class": [2, 0],
        },
        {
            "lat": [-3.3],
            "lon": [-52.3],
            "alert_date": ["2026-10-02"],
            "alert_confidence": ["high"],
            "natural_lands_class": [5],
        },
    ]
    first, second = repository.queries
    assert (first.start_date, first.end_date) == (date(2026, 10, 1), date(2026, 10, 18))
    assert first.confidences == [3, 4]
    assert first.natural_lands_classes == [2, 5, 8, 9]
    assert (first.after, second.after) == (None, "1")
    assert analysis.metadata["alerts"]["version"] == "v20261020"
