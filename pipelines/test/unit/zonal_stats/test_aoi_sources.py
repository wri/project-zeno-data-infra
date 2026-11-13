import pickle

import geopandas as gpd
import pytest
from pyiceberg.expressions import EqualTo
from shapely.geometry import box

from pipelines.aois.stages import write_geoparquet
from pipelines.test.unit.conftest import custom_aoi_rows
from pipelines.zonal_stats.aoi_sources import (
    GeoParquetSource,
    IcebergSource,
    aoi_source_for,
    custom_aoi_source,
    predefined_aoi_sources,
)

AOIS = {
    "near": box(0, 0, 1, 1),
    "far": box(5, 5, 6, 6),
    "also-near": box(0.5, 0.5, 2, 2),
}


@pytest.fixture
def geoparquet_source(tmp_path):
    uri = str(tmp_path / "aois.parquet")
    features = gpd.GeoDataFrame(
        {"aoi_id": list(AOIS), "adm_level": [0, 1, 2]},
        geometry=list(AOIS.values()),
        crs="EPSG:4326",
    )
    write_geoparquet(features, uri, row_group_bytes=100)
    return GeoParquetSource(uri, "protected_area")


def test_custom_aoi_source_reads_bounds_and_nearby_polygons(custom_aoi_table):
    custom_aoi_table.append(
        custom_aoi_rows(
            {
                "near": box(0, 0, 1, 1),
                "far": box(5, 5, 6, 6),
                "also-near": box(0.5, 0.5, 2, 2),
            }
        )
    )
    source = custom_aoi_source()

    bounds = source.read_bounds().set_index("aoi_id")
    assert sorted(bounds.index) == ["also-near", "far", "near"]
    assert bounds.loc["far"].tolist() == [5, 5, 6, 6]

    read = source.read((0.5, 0.5, 0.6, 0.6), ["near", "also-near", "far"]).set_index(
        "aoi_id"
    )
    assert sorted(read.index) == ["also-near", "near"]
    assert read.geometry.loc["near"].equals(box(0, 0, 1, 1))
    assert read.crs == "EPSG:4326"
    assert source.read((10, 10, 11, 11), ["near", "far"]).empty
    assert source.provenance == {
        "aoi_source": "iceberg:aois/custom.custom_aois",
        "aoi_snapshot_id": str(custom_aoi_table.current_snapshot().snapshot_id),
    }


def test_iceberg_read_returns_only_the_requested_ids(custom_aoi_table):
    custom_aoi_table.append(custom_aoi_rows(AOIS))

    read = custom_aoi_source().read((0, 0, 1, 1), ["also-near", "far"])

    assert read["aoi_id"].tolist() == ["also-near"]


def test_pinned_source_keeps_reading_its_snapshot(custom_aoi_table):
    custom_aoi_table.append(custom_aoi_rows({"a": box(0, 0, 1, 1)}))
    pinned = custom_aoi_source()

    custom_aoi_table.append(custom_aoi_rows({"b": box(0, 0, 1, 1)}))
    custom_aoi_table.delete(EqualTo("aoi_id", "a"))

    assert pinned.read_bounds()["aoi_id"].tolist() == ["a"]
    assert pinned.read((0, 0, 1, 1), ["a", "b"])["aoi_id"].tolist() == ["a"]
    assert pinned.pin() == pinned  # already pinned: not moved to the new snapshot
    assert custom_aoi_source().read_bounds()["aoi_id"].tolist() == ["b"]


def test_unpinned_source_refuses_to_read(custom_aoi_table):
    custom_aoi_table.append(custom_aoi_rows({"a": box(0, 0, 1, 1)}))
    source = IcebergSource(
        catalog="aois", table="custom.custom_aois", aoi_type="custom"
    )

    with pytest.raises(RuntimeError, match="before reading"):
        source.read_bounds()
    assert source.pin().read_bounds()["aoi_id"].tolist() == ["a"]


def test_source_without_saved_aois_is_empty(custom_aoi_table):
    source = custom_aoi_source()

    assert source.snapshot_id is None
    assert source.read_bounds().empty
    assert list(source.read_bounds().columns) == [
        "aoi_id",
        "xmin",
        "ymin",
        "xmax",
        "ymax",
    ]
    assert source.read((0, 0, 1, 1), ["a"]).empty


def test_source_survives_pickling_for_dask_tasks(custom_aoi_table):
    custom_aoi_table.append(custom_aoi_rows({"a": box(0, 0, 1, 1)}))
    source = custom_aoi_source()

    copy = pickle.loads(pickle.dumps(source))

    assert isinstance(copy, IcebergSource)
    assert copy == source
    assert copy.read((0, 0, 1, 1), ["a"])["aoi_id"].tolist() == ["a"]


def test_geoparquet_bounds_are_the_covering_bboxes(geoparquet_source):
    bounds = geoparquet_source.read_bounds().set_index("aoi_id").sort_index()

    assert list(bounds.columns) == ["xmin", "ymin", "xmax", "ymax"]
    assert bounds.loc["also-near"].tolist() == [0.5, 0.5, 2, 2]
    assert sorted(bounds.index) == sorted(AOIS)


def test_geoparquet_read_filters_by_bbox_and_ids(geoparquet_source):
    read = geoparquet_source.read((0.5, 0.5, 0.6, 0.6), ["near", "also-near", "far"])

    assert sorted(read["aoi_id"]) == ["also-near", "near"]
    assert read.crs == "EPSG:4326"
    assert geoparquet_source.read((0.5, 0.5, 0.6, 0.6), ["far"]).empty
    assert (
        geoparquet_source.read((0, 0, 1, 1), ["near"])
        .geometry.iloc[0]
        .equals(AOIS["near"])
    )


def test_geoparquet_source_attributes_and_pickling(geoparquet_source):
    attributes = geoparquet_source.read_attributes(["aoi_id", "adm_level"])
    copy = pickle.loads(pickle.dumps(geoparquet_source))

    assert sorted(attributes["aoi_id"]) == sorted(AOIS)
    assert copy == geoparquet_source
    assert copy.provenance == {"aoi_source": geoparquet_source.uri}


def test_predefined_aoi_sources_read_the_aoi_pipelines_geoparquets():
    sources = {source.aoi_type: source.uri for source in predefined_aoi_sources()}

    vectors = "s3://lcl-analytics/vectors/aois"
    assert sources == {
        "admin": f"{vectors}/gadm/v4.1.85/aois.parquet",
        "protected_area": f"{vectors}/wdpa/v202512/aois.parquet",
        "key_biodiversity_area": f"{vectors}/kba/v20240904/aois.parquet",
        "indigenous_land": f"{vectors}/landmark/v20260617/aois.parquet",
    }


def test_aoi_source_for_builds_only_the_requested_type(custom_aoi_table, monkeypatch):
    # The admin source is read without touching the custom AOI catalog.
    monkeypatch.delenv("PYICEBERG_CATALOG__AOIS__URI")
    admin = aoi_source_for("admin")
    assert isinstance(admin, GeoParquetSource) and admin.aoi_type == "admin"
