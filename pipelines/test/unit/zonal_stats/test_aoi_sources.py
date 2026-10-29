import pickle

from pyiceberg.expressions import EqualTo
from shapely.geometry import box

from pipelines.test.unit.conftest import custom_aoi_rows
from pipelines.zonal_stats.aoi_sources import IcebergSource, custom_aois


def test_custom_aois_bounds_and_read_return_monitored_aois(custom_aoi_table):
    custom_aoi_table.append(
        custom_aoi_rows(
            {
                "near": box(0, 0, 1, 1),
                "far": box(5, 5, 6, 6),
                "unmonitored": box(0.5, 0.5, 2, 2),
            },
            not_monitored=["unmonitored"],
        )
    )
    source = custom_aois()

    bounds = source.bounds().set_index("aoi_id")
    assert sorted(bounds.index) == ["far", "near"]
    assert bounds.loc["far"].tolist() == [5, 5, 6, 6]

    read = source.read((0.5, 0.5, 0.6, 0.6))
    assert read["aoi_id"].tolist() == ["near"]
    assert read.geometry.iloc[0].equals(box(0, 0, 1, 1))
    assert read.crs == "EPSG:4326"
    assert source.read((10, 10, 11, 11)).empty
    assert source.provenance() == {"aoi_source": "iceberg:aois/custom.custom_aois"}


def test_source_reads_the_current_table(custom_aoi_table):
    source = custom_aois()
    custom_aoi_table.append(custom_aoi_rows({"a": box(0, 0, 1, 1)}))
    assert source.read((0, 0, 1, 1))["aoi_id"].tolist() == ["a"]

    custom_aoi_table.append(custom_aoi_rows({"b": box(0, 0, 1, 1)}))
    custom_aoi_table.delete(EqualTo("aoi_id", "a"))

    assert source.bounds()["aoi_id"].tolist() == ["b"]
    assert source.read((0, 0, 1, 1))["aoi_id"].tolist() == ["b"]


def test_source_without_saved_aois_is_empty(custom_aoi_table):
    source = custom_aois()

    assert source.bounds().empty
    assert list(source.bounds().columns) == ["aoi_id", "xmin", "ymin", "xmax", "ymax"]
    assert source.read((0, 0, 1, 1)).empty


def test_source_survives_pickling_for_dask_tasks(custom_aoi_table):
    custom_aoi_table.append(custom_aoi_rows({"a": box(0, 0, 1, 1)}))
    source = custom_aois()

    copy = pickle.loads(pickle.dumps(source))

    assert isinstance(copy, IcebergSource)
    assert copy == source
    assert copy.read((0, 0, 1, 1))["aoi_id"].tolist() == ["a"]
