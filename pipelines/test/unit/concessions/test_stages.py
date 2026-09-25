from unittest.mock import patch

import geopandas as gpd
import pyarrow.parquet as pq
import pytest
from pipelines.concessions.concession_source import ConcessionSource
from pipelines.concessions.stages import (
    build_pmtiles,
    normalize,
    with_area_ha,
    write_geoparquet,
)
from shapely import box

SOURCE = ConcessionSource(
    concession_type="oil_palm",
    source_uri="s3://bucket/{version}/concessions.zip",
    dropped_columns=("shape_leng", "shape_area"),
)


@pytest.fixture
def raw_features():
    return gpd.GeoDataFrame(
        {
            "ISO3": ["IDN", "MYS"],
            "Company": ["A", "B"],
            "Shape_Leng": [4.0, 4.0],
            "Shape_Area": [1.0, 1.0],
        },
        geometry=[box(0, 0, 1, 1), box(1, 1, 2, 2)],
        crs="EPSG:4326",
    )


def test_normalize_assigns_row_number_ids_and_concession_columns(raw_features):
    normalized = normalize(raw_features, SOURCE)

    assert list(normalized.aoi_id) == ["1", "2"]
    assert set(normalized.aoi_type) == {"concession"}
    assert set(normalized.concession_type) == {"oil_palm"}
    assert list(normalized.columns) == [
        "aoi_id",
        "aoi_type",
        "concession_type",
        "iso3",
        "company",
        "geometry",
    ]


def test_normalize_reprojects_to_wgs84(raw_features):
    normalized = normalize(raw_features.to_crs("EPSG:3857"), SOURCE)

    assert normalized.crs == "EPSG:4326"


def test_area_is_geodesic_hectares(raw_features):
    # a 1x1 degree cell at the equator is ~1,230,000 ha
    area_ha = with_area_ha(raw_features).area_ha.iloc[0]

    assert area_ha == pytest.approx(1_230_000, rel=0.01)


def test_source_uri_resolves_version():
    assert SOURCE.uri_for("v2025") == "s3://bucket/v2025/concessions.zip"


def test_geoparquet_round_trips_features_and_crs(raw_features, tmp_path):
    uri = str(tmp_path / "concessions.parquet")
    features = normalize(raw_features, SOURCE)

    write_geoparquet(features, uri)
    loaded = gpd.read_parquet(uri)

    assert loaded.crs == "EPSG:4326"
    assert list(loaded.aoi_id) == ["1", "2"]
    assert "bbox" in pq.read_schema(uri).names


@patch("pipelines.concessions.stages.subprocess.run")
def test_pmtiles_are_built_from_features_with_tippecanoe(run, raw_features, tmp_path):
    output_path = tmp_path / "concessions.pmtiles"

    build_pmtiles(normalize(raw_features, SOURCE), output_path, layer="concessions")

    command = run.call_args.args[0]
    input_path = command[-1]
    assert command[0] == "tippecanoe"
    assert f"--output={output_path}" in command
    assert "--layer=concessions" in command
    assert run.call_args.kwargs["check"] is True
    assert '"aoi_id": "1"' in open(input_path).read()
