import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import pytest
from shapely import MultiPolygon, box

from pipelines.aois.aoi_source import AoiSource
from pipelines.aois.stages import (
    build_search_index,
    deduplicate,
    gdal_path,
    normalize,
    with_area_ha,
    with_extent,
    write_geoparquet,
)

SOURCE = AoiSource(
    source="wdpa",
    version="v202512",
    uri="s3://bucket/wdpa.gdb.zip",
    layer="WDPA_poly",
    id_column="site_pid",
    subtype="protected-area",
    name_columns=("name", "desig", "iso3"),
    iso3_column="iso3",
)


@pytest.fixture
def raw_features():
    return gpd.GeoDataFrame(
        {
            "SITE_PID": [101, 102],
            "NAME": ["Yosemite", "Iguazu"],
            "DESIG": ["National Park", None],
            "ISO3": ["USA", "ARG;BRA"],
        },
        geometry=[box(0, 0, 1, 1), box(1, 1, 2, 2)],
        crs="EPSG:4326",
    )


def features(ids, geometries, **columns):
    return gpd.GeoDataFrame(
        {"aoi_id": ids, **columns}, geometry=geometries, crs="EPSG:4326"
    )


def test_normalize_assigns_string_ids_and_source_columns(raw_features):
    normalized = normalize(raw_features, SOURCE)

    assert list(normalized.aoi_id) == ["101", "102"]
    assert set(normalized.source) == {"wdpa"}
    assert set(normalized.subtype) == {"protected-area"}


def test_normalize_joins_name_columns_skipping_missing_values(raw_features):
    raw_features.loc[1, "ISO3"] = ""

    normalized = normalize(raw_features, SOURCE)

    assert list(normalized.name) == ["Yosemite, National Park, USA", "Iguazu"]


def test_normalize_splits_multi_country_iso3(raw_features):
    normalized = normalize(raw_features, SOURCE)

    assert [list(codes) for codes in normalized.iso3] == [["USA"], ["ARG", "BRA"]]


def test_normalize_keeps_clashing_source_attributes_under_source_prefix(
    raw_features,
):
    normalized = normalize(raw_features, SOURCE)

    assert list(normalized.wdpa_name) == ["Yosemite", "Iguazu"]
    assert list(normalized.wdpa_iso3) == ["USA", "ARG;BRA"]
    assert normalized.columns.is_unique


def test_normalize_reprojects_to_wgs84(raw_features):
    normalized = normalize(raw_features.to_crs("EPSG:3857"), SOURCE)

    assert normalized.crs == "EPSG:4326"


def test_area_is_geodesic_hectares():
    # a 1x1 degree cell at the equator is ~1,230,000 ha
    area_ha = with_area_ha(features(["1"], [box(0, 0, 1, 1)])).area_ha.iloc[0]

    assert area_ha == pytest.approx(1_230_000, rel=0.01)


def test_deduplicate_keeps_largest_feature_per_id():
    duplicated = features(
        ["1", "1", "2"], [box(0, 0, 1, 1), box(0, 0, 2, 2), box(0, 0, 1, 1)]
    )

    deduplicated = deduplicate(with_area_ha(duplicated))

    assert sorted(deduplicated.aoi_id) == ["1", "2"]
    assert deduplicated.set_index("aoi_id").geometry["1"].area == 4.0


def test_extent_is_bounds_for_ordinary_geometry():
    extent = with_extent(features(["1"], [box(10, -5, 20, 5)])).extent.iloc[0]

    assert list(extent) == [10, -5, 20, 5]


def test_extent_wraps_across_antimeridian():
    fiji = MultiPolygon([box(177, -19, 180, -16), box(-180, -19, -178, -16)])

    extent = with_extent(features(["1"], [fiji])).extent.iloc[0]

    assert list(extent) == [177, -19, -178, -16]


def test_gdal_path_reads_zipped_datasets_through_zip_file_system():
    assert gdal_path("/tmp/file.gdb.zip") == "/vsizip//tmp/file.gdb.zip"
    assert gdal_path("/tmp/file.gpkg") == "/tmp/file.gpkg"


def test_geoparquet_round_trips_with_covering_bbox(tmp_path):
    uri = str(tmp_path / "aois.parquet")

    write_geoparquet(features(["2", "1"], [box(0, 0, 1, 1), box(1, 1, 2, 2)]), uri)

    assert gpd.read_parquet(uri).crs == "EPSG:4326"
    assert "bbox" in pq.read_schema(uri).names


def test_search_index_combines_sources_without_geometry(tmp_path):
    uris = []
    for source in ("wdpa", "kba"):
        uri = str(tmp_path / f"{source}.parquet")
        write_geoparquet(
            features(["1"], [box(0, 0, 1, 1)], source=[source], name=["A"]), uri
        )
        uris.append(uri)

    index = build_search_index(uris, columns=["source", "aoi_id", "name"])

    assert isinstance(index, pd.DataFrame)
    assert not isinstance(index, gpd.GeoDataFrame)
    assert index.to_dict("records") == [
        {"source": "wdpa", "aoi_id": "1", "name": "A"},
        {"source": "kba", "aoi_id": "1", "name": "A"},
    ]
