import geopandas as gpd
import pytest
from shapely.geometry import box

from app.infrastructure.persistence.geoparquet_aoi_geometry_repository import (
    GeoParquetAoiGeometryRepository,
)
from app.models.common.areas_of_interest import ProtectedAreaOfInterest


@pytest.fixture
def geoparquet_uri(tmp_path):
    uri = str(tmp_path / "aois.parquet")
    gpd.GeoDataFrame(
        {"aoi_id": ["1", "2", "3"], "area_ha": [10.0, 20.0, 30.0]},
        geometry=[box(0, 0, 1, 1), box(0, 0, 2, 2), box(0, 0, 3, 3)],
        crs="EPSG:4326",
    ).to_parquet(uri, write_covering_bbox=True)
    return uri


@pytest.mark.asyncio
async def test_geometries_are_returned_in_requested_order(geoparquet_uri):
    repository = GeoParquetAoiGeometryRepository(geoparquet_uri)

    geometries, areas_ha = await repository.load(
        ProtectedAreaOfInterest(ids=["3", "1"])
    )

    assert [geometry.area for geometry in geometries] == [9.0, 1.0]
    assert areas_ha == [30.0, 10.0]


@pytest.mark.asyncio
async def test_unknown_ids_raise(geoparquet_uri):
    repository = GeoParquetAoiGeometryRepository(geoparquet_uri)

    with pytest.raises(ValueError, match="99"):
        await repository.load(ProtectedAreaOfInterest(ids=["1", "99"]))
