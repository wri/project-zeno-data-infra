import geopandas as gpd
import pytest
from shapely.geometry import box

from app.infrastructure.persistence.geoparquet_admin_subregions import (
    GeoParquetAdminSubregions,
)


@pytest.fixture
def gadm_uri(tmp_path):
    uri = str(tmp_path / "gadm.parquet")
    gpd.GeoDataFrame(
        {
            "aoi_id": ["IDN", "IDN.1", "IDN.10", "IDN.1.2", "IDN.10.3", "MYS", "Z01"],
            "gid_0": ["IDN", "IDN", "IDN", "IDN", "IDN", "MYS", "Z01"],
            "adm_level": [0, 1, 1, 2, 2, 0, 0],
            "country": ["Indonesia"] * 5 + ["Malaysia", "Disputed"],
            "name_1": [None, "Aceh", "Jawa Tengah", "Aceh", "Jawa Tengah", None, None],
            "name_2": [None, None, None, "Bener Meriah", "Banyumas", None, None],
            "is_disputed": [False] * 6 + [True],
            "area_ha": [100.0, 10.0, 20.0, 1.0, 2.0, 50.0, 5.0],
        },
        geometry=[box(0, 0, 1, 1)] * 7,
        crs="EPSG:4326",
    ).to_parquet(uri, write_covering_bbox=True)
    return uri


@pytest.mark.asyncio
async def test_lists_the_next_level_of_a_country(gadm_uri):
    subregions = await GeoParquetAdminSubregions(gadm_uri).within("IDN", 1)

    names = {subregion.aoi_id: subregion.name for subregion in subregions}
    assert names == {"IDN.1": "Aceh", "IDN.10": "Jawa Tengah"}


@pytest.mark.asyncio
async def test_matches_whole_id_segments_below_a_state(gadm_uri):
    subregions = await GeoParquetAdminSubregions(gadm_uri).within("IDN.1", 2)

    assert [subregion.aoi_id for subregion in subregions] == ["IDN.1.2"]


@pytest.mark.asyncio
async def test_lists_two_levels_below_a_country(gadm_uri):
    subregions = await GeoParquetAdminSubregions(gadm_uri).within("IDN", 2)

    assert {subregion.name for subregion in subregions} == {
        "Bener Meriah",
        "Banyumas",
    }


@pytest.mark.asyncio
async def test_lists_countries_without_a_parent(gadm_uri):
    subregions = await GeoParquetAdminSubregions(gadm_uri).within(None, 0)

    assert {subregion.name for subregion in subregions} == {"Indonesia", "Malaysia"}


@pytest.mark.asyncio
async def test_countries_leave_out_disputed_areas(gadm_uri):
    subregions = await GeoParquetAdminSubregions(gadm_uri).within(None, 0)

    assert {subregion.aoi_id for subregion in subregions} == {"IDN", "MYS"}
