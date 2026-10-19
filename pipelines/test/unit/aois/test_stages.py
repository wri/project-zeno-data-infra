import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import pytest
from shapely import box

from pipelines.aois.aoi_source import AoiSource, column_ids
from pipelines.aois.stages import (
    drop_missing_ids,
    normalize,
    removed,
    with_area_ha,
    write_geoparquet,
)

SOURCE = AoiSource(
    source="wdpa",
    version="v202512",
    load=gpd.GeoDataFrame,
    aoi_ids=column_ids("site_pid"),
)


@pytest.fixture
def raw_features():
    return gpd.GeoDataFrame(
        {"SITE_PID": [101, 102], "NAME": ["Yosemite", "Iguazu"]},
        geometry=[box(0, 0, 1, 1), box(1, 1, 2, 2)],
        crs="EPSG:4326",
    )


def features(ids, geometries, **columns):
    return gpd.GeoDataFrame(
        {"aoi_id": ids, **columns}, geometry=geometries, crs="EPSG:4326"
    )


def test_normalize_assigns_string_ids_and_keeps_lowercased_attributes(
    raw_features,
):
    normalized = normalize(raw_features, SOURCE)

    assert list(normalized.aoi_id) == ["101", "102"]
    assert list(normalized.name) == ["Yosemite", "Iguazu"]


def test_missing_ids_stay_missing_instead_of_becoming_strings():
    attributes = pd.DataFrame({"sitrecid": [101, None]})

    assert column_ids("sitrecid")(attributes).tolist() == ["101", pd.NA]


def test_rows_without_an_id_are_dropped():
    identified = features(["1", None], [box(0, 0, 1, 1), box(1, 1, 2, 2)])

    assert list(drop_missing_ids(identified).aoi_id) == ["1"]


def test_removed_rows_are_reported_with_reason_and_without_geometry():
    before = features(["1", "2"], [box(0, 0, 1, 1), box(1, 1, 2, 2)], name=["A", "B"])
    after = before.iloc[[0]]

    report = removed(before, after, "missing_id")

    assert report.to_dict("records") == [
        {"aoi_id": "2", "name": "B", "reason": "missing_id"}
    ]


def test_normalize_reprojects_to_wgs84(raw_features):
    normalized = normalize(raw_features.to_crs("EPSG:3857"), SOURCE)

    assert normalized.crs == "EPSG:4326"


def test_area_is_geodesic_hectares():
    # a 1x1 degree cell at the equator is ~1,230,000 ha
    area_ha = with_area_ha(features(["1"], [box(0, 0, 1, 1)])).area_ha.iloc[0]

    assert area_ha == pytest.approx(1_230_000, rel=0.01)


def test_geoparquet_round_trips_with_covering_bbox(tmp_path):
    uri = str(tmp_path / "aois.parquet")

    write_geoparquet(features(["2", "1"], [box(0, 0, 1, 1), box(1, 1, 2, 2)]), uri)

    assert gpd.read_parquet(uri).crs == "EPSG:4326"
    assert "bbox" in pq.read_schema(uri).names


def test_geoparquet_row_groups_hold_non_overlapping_id_ranges(tmp_path):
    # lets id-filtered reads skip every row group that can't hold a requested id
    uri = str(tmp_path / "aois.parquet")
    ids = [str(i) for i in range(10)]

    write_geoparquet(features(ids[::-1], [box(0, 0, 1, 1)] * 10), uri, row_group_size=3)

    metadata = pq.ParquetFile(uri).metadata
    id_column = metadata.schema.names.index("aoi_id")
    ranges = [
        (
            metadata.row_group(group).column(id_column).statistics.min,
            metadata.row_group(group).column(id_column).statistics.max,
        )
        for group in range(metadata.num_row_groups)
    ]
    assert len(ranges) == 4
    assert all(left[1] < right[0] for left, right in zip(ranges, ranges[1:]))
