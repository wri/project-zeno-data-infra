import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import pytest
from shapely import Polygon, box

from pipelines.aois.aoi_source import AoiSource, column_ids
from pipelines.aois.geometry_repair import repair_geometries
from pipelines.aois.stages import (
    changed_aois,
    drop_missing_ids,
    normalize,
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


def test_changed_aois_report_removed_and_repaired_features():
    bowtie = Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])
    collinear = Polygon([(0, 0), (1, 1), (2, 2), (0, 0)])
    normalized = features(
        [None, "2", "3", "4"],
        [box(0, 0, 1, 1), bowtie, collinear, box(0, 0, 1, 1)],
        name=["A", "B", "C", "D"],
    )
    identified = drop_missing_ids(normalized)
    _, repair_report = repair_geometries(identified)

    report = changed_aois(normalized, identified, repair_report)

    assert report.drop(columns="detail").to_dict("records") == [
        {
            "aoi_id": None,
            "change": "removed",
            "reason": "missing_id",
            "source_attributes": '{"name":"A"}',
        },
        {
            "aoi_id": "2",
            "change": "repaired",
            "reason": "invalid_geometry",
            "source_attributes": '{"name":"B"}',
        },
        {
            "aoi_id": "3",
            "change": "removed",
            "reason": "invalid_geometry",
            "source_attributes": '{"name":"C"}',
        },
    ]
    assert report.detail.tolist()[1] == "Self-intersection[1 1]"


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


def test_changed_aois_is_empty_with_columns_when_nothing_changed():
    normalized = features(["1"], [box(0, 0, 1, 1)], name=["A"])
    identified = drop_missing_ids(normalized)
    _, repair_report = repair_geometries(identified)

    report = changed_aois(normalized, identified, repair_report)

    assert report.empty
    assert list(report.columns) == [
        "aoi_id",
        "change",
        "reason",
        "detail",
        "source_attributes",
    ]
