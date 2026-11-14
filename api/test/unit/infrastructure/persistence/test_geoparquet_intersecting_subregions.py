import geopandas as gpd
import pytest
from shapely.geometry import box

from app.infrastructure.persistence.geoparquet_intersecting_subregions import (
    GeoParquetIntersectingSubregions,
)


@pytest.fixture
def protected_areas_uri(tmp_path):
    uri = str(tmp_path / "wdpa.parquet")
    gpd.GeoDataFrame(
        {
            "aoi_id": ["inside", "border", "outside", "straddling"],
            "name": ["Inside", "Border", "Outside", "Straddling"],
            "area_ha": [1.0, 2.0, 3.0, 4.0],
        },
        geometry=[
            box(1, 1, 2, 2),
            box(10, 0, 11, 10),
            box(50, 50, 51, 51),
            box(8, 8, 12, 9),
        ],
        crs="EPSG:4326",
    ).to_parquet(uri, write_covering_bbox=True)
    return uri


@pytest.mark.asyncio
async def test_keeps_areas_inside_the_parent_only(protected_areas_uri):
    subregions = await GeoParquetIntersectingSubregions(
        protected_areas_uri, "protected_area", "name"
    ).within(box(0, 0, 10, 10))

    assert [subregion.aoi_id for subregion in subregions] == ["inside"]
    assert subregions[0].aoi_type == "protected_area"
    assert subregions[0].name == "Inside"
