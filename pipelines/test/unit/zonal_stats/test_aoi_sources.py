import pickle

import pytest
from pyiceberg.expressions import EqualTo
from shapely.geometry import box

from pipelines.test.unit.conftest import custom_aoi_rows
from pipelines.zonal_stats.aoi_sources import IcebergSource, custom_aoi_source


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

    read = source.read((0.5, 0.5, 0.6, 0.6)).set_index("aoi_id")
    assert sorted(read.index) == ["also-near", "near"]
    assert read.geometry.loc["near"].equals(box(0, 0, 1, 1))
    assert read.crs == "EPSG:4326"
    assert source.read((10, 10, 11, 11)).empty
    assert source.provenance == {
        "aoi_source": "iceberg:aois/custom.custom_aois",
        "aoi_snapshot_id": str(custom_aoi_table.current_snapshot().snapshot_id),
    }


def test_pinned_source_keeps_reading_its_snapshot(custom_aoi_table):
    custom_aoi_table.append(custom_aoi_rows({"a": box(0, 0, 1, 1)}))
    pinned = custom_aoi_source()

    custom_aoi_table.append(custom_aoi_rows({"b": box(0, 0, 1, 1)}))
    custom_aoi_table.delete(EqualTo("aoi_id", "a"))

    assert pinned.read_bounds()["aoi_id"].tolist() == ["a"]
    assert pinned.read((0, 0, 1, 1))["aoi_id"].tolist() == ["a"]
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
    assert source.read((0, 0, 1, 1)).empty


def test_source_survives_pickling_for_dask_tasks(custom_aoi_table):
    custom_aoi_table.append(custom_aoi_rows({"a": box(0, 0, 1, 1)}))
    source = custom_aoi_source()

    copy = pickle.loads(pickle.dumps(source))

    assert isinstance(copy, IcebergSource)
    assert copy == source
    assert copy.read((0, 0, 1, 1))["aoi_id"].tolist() == ["a"]
