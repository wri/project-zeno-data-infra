from test.unit.alert_points import (
    DAYS_STORED,
    GRID,
    LATEST_DAY,
    aoi_polygon,
    brute_force,
    lat,
    lon,
    make_points,
    write_points_table,
)

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
import shapely

from app.infrastructure.external_services import alert_points_table
from app.infrastructure.external_services.alert_points_table import (
    date_to_day,
    day_to_date,
    load_alert_points_table,
    newest_points,
)

ALL_CONFIDENCES = [2, 3, 4]
POINT_COLUMNS = ["row", "col", "alert_date", "confidence", "natural_lands_class"]


@pytest.fixture
def points():
    return make_points()


@pytest.fixture
def table(tmp_path, points):
    load_alert_points_table.cache_clear()
    yield load_alert_points_table(
        write_points_table(tmp_path / "by-date.parquet", points)
    )
    load_alert_points_table.cache_clear()


def _same_rows(a, b):
    return (
        a[POINT_COLUMNS]
        .reset_index(drop=True)
        .astype(b[POINT_COLUMNS].dtypes)
        .equals(b[POINT_COLUMNS].reset_index(drop=True))
    )


def test_load_alert_points_table_reads_grid_and_window_from_metadata(table):
    assert table.version == "v20261018"
    assert table.latest_day == LATEST_DAY
    assert table.first_day == LATEST_DAY - DAYS_STORED + 1
    assert table.lon(10) == pytest.approx(GRID["x0"] + 10 * GRID["dx"])
    assert table.lat(10) == pytest.approx(GRID["y0"] + 10 * GRID["dy"])


def test_table_without_metadata_falls_back_to_path_statistics_and_grid(
    tmp_path, points
):
    path = tmp_path / "v20261018" / "by-date.parquet"
    path.parent.mkdir()
    with_metadata = pq.read_table(write_points_table(path, points))
    pq.write_table(with_metadata.replace_schema_metadata(None), path)
    load_alert_points_table.cache_clear()
    try:
        table = load_alert_points_table(str(path))
    finally:
        load_alert_points_table.cache_clear()

    assert table.version == "v20261018"
    assert table.latest_day == points.alert_date.max()
    assert table.first_day == points.alert_date.min()
    assert table.lon(0) == pytest.approx(-179.99995)
    assert table.lat(0) == pytest.approx(89.99995)
    assert table.lon(3_599_999) == pytest.approx(179.99995)
    assert table.lat(1_499_999) == pytest.approx(-59.99995)


def test_table_without_metadata_or_version_in_path_is_refused(tmp_path, points):
    path = tmp_path / "by-date.parquet"
    with_metadata = pq.read_table(write_points_table(path, points))
    pq.write_table(with_metadata.replace_schema_metadata(None), path)
    load_alert_points_table.cache_clear()
    try:
        with pytest.raises(ValueError, match="No alerts version"):
            load_alert_points_table(str(path))
    finally:
        load_alert_points_table.cache_clear()


def test_table_fixture_has_interior_and_boundary_row_groups(table):
    groups = np.arange(table.metadata.num_row_groups)
    boxes = table.boxes(groups)
    geom = aoi_polygon()
    interior = shapely.contains_properly(geom, boxes)
    boundary = shapely.intersects(geom, boxes) & ~interior
    assert interior.any() and boundary.any()
    assert (table.date_min == table.date_max).all()


@pytest.mark.parametrize(
    "start_day, end_day, confidences, classes",
    [
        (LATEST_DAY - DAYS_STORED + 1, LATEST_DAY, ALL_CONFIDENCES, None),
        (LATEST_DAY - 1, LATEST_DAY, [3, 4], None),
        (LATEST_DAY - DAYS_STORED + 1, LATEST_DAY, ALL_CONFIDENCES, list(range(2, 12))),
        (LATEST_DAY - 2, LATEST_DAY - 2, [2], [2, 5, 8, 9]),
    ],
)
def test_all_points_match_brute_force(
    table, points, start_day, end_day, confidences, classes
):
    geom = aoi_polygon()
    expected = brute_force(points, geom, start_day, end_day, confidences, classes)

    result, truncated, _ = newest_points(
        table, geom, start_day, end_day, confidences, classes, limit=len(points)
    )

    assert len(expected) > 0
    assert _same_rows(result, expected)
    assert not truncated


@pytest.mark.parametrize("limit", [1, 37, 400])
def test_limit_returns_the_newest_points(table, points, limit):
    geom = aoi_polygon()
    start_day = LATEST_DAY - DAYS_STORED + 1
    expected = brute_force(points, geom, start_day, LATEST_DAY, ALL_CONFIDENCES, None)

    result, truncated, _ = newest_points(
        table, geom, start_day, LATEST_DAY, ALL_CONFIDENCES, None, limit=limit
    )

    assert _same_rows(result, expected.head(limit))
    assert truncated


def test_limit_equal_to_matches_is_not_truncated(table, points):
    geom = aoi_polygon()
    start_day = LATEST_DAY - DAYS_STORED + 1
    expected = brute_force(points, geom, start_day, LATEST_DAY, ALL_CONFIDENCES, None)

    result, truncated, _ = newest_points(
        table, geom, start_day, LATEST_DAY, ALL_CONFIDENCES, None, limit=len(expected)
    )

    assert _same_rows(result, expected)
    assert not truncated


def test_limit_reads_only_the_newest_dates(table, monkeypatch):
    geom = aoi_polygon()
    read_dates = []
    read_row_groups = pq.ParquetFile.read_row_groups

    def recording_read_row_groups(self, row_groups, *args, **kwargs):
        read_dates.extend(table.date_max[row_groups].tolist())
        return read_row_groups(self, row_groups, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "read_row_groups", recording_read_row_groups)
    newest_points(
        table,
        geom,
        LATEST_DAY - DAYS_STORED + 1,
        LATEST_DAY,
        ALL_CONFIDENCES,
        None,
        limit=5,
    )

    assert set(read_dates) == {LATEST_DAY}


def test_small_page_reads_only_part_of_the_newest_date(table, points, monkeypatch):
    geom = aoi_polygon()
    read_groups = []
    read_row_groups = pq.ParquetFile.read_row_groups

    def recording_read_row_groups(self, row_groups, *args, **kwargs):
        read_groups.extend(row_groups)
        return read_row_groups(self, row_groups, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "read_row_groups", recording_read_row_groups)
    monkeypatch.setattr(alert_points_table, "READ_BATCH_GROUPS", 2)
    start_day = LATEST_DAY - DAYS_STORED + 1
    result, truncated, _ = newest_points(
        table, geom, start_day, LATEST_DAY, ALL_CONFIDENCES, None, limit=5
    )

    newest_date_groups = np.flatnonzero(
        (table.date_max == LATEST_DAY)
        & shapely.intersects(
            geom, table.boxes(np.arange(table.metadata.num_row_groups))
        )
    )
    assert 0 < len(read_groups) < len(newest_date_groups)
    assert read_groups == sorted(read_groups)
    expected = brute_force(points, geom, start_day, LATEST_DAY, ALL_CONFIDENCES, None)
    assert _same_rows(result, expected.head(5))
    assert truncated


@pytest.mark.parametrize(
    "page_size, confidences, classes",
    [
        (1, ALL_CONFIDENCES, None),
        (37, ALL_CONFIDENCES, None),
        (25, [3, 4], list(range(2, 12))),
    ],
)
def test_pages_after_the_last_position_join_into_the_full_list(
    table, points, monkeypatch, page_size, confidences, classes
):
    monkeypatch.setattr(alert_points_table, "READ_BATCH_GROUPS", 2)
    geom = aoi_polygon()
    start_day = LATEST_DAY - DAYS_STORED + 1
    expected = brute_force(points, geom, start_day, LATEST_DAY, confidences, classes)

    pages, after, truncated = [], None, True
    while truncated:
        page, truncated, last = newest_points(
            table,
            geom,
            start_day,
            LATEST_DAY,
            confidences,
            classes,
            limit=page_size,
            after=after,
        )
        assert len(page) == page_size or not truncated
        pages.append(page)
        after = last

    assert _same_rows(pd.concat(pages, ignore_index=True), expected)


def test_aoi_without_alerts_returns_no_points(table):
    far_away = shapely.box(lon(0), lat(100), lon(100), lat(0))
    result, truncated, _ = newest_points(
        table, far_away, table.first_day, table.latest_day, ALL_CONFIDENCES, None, 10
    )
    assert result.empty
    assert list(result.columns) == POINT_COLUMNS
    assert not truncated


def test_date_conversions_round_trip():
    assert day_to_date(LATEST_DAY).isoformat() == "2026-10-16"
    assert date_to_day("2026-10-16") == LATEST_DAY
    assert date_to_day("2026-1-5") == date_to_day("2026-01-05")
