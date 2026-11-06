import pytest
from shapely.geometry import MultiPolygon, box

from app.domain.models.custom_aoi import CustomAoiGeometry
from app.domain.repositories.aoi_geometry_repository import AoiNotFoundError
from app.domain.repositories.custom_aoi_geometry_repository import (
    CustomAoiGeometryRepository,
)
from app.models.common.areas_of_interest import SavedCustomAreaOfInterest


class FakeCustomAoiRepository:
    async def get(self, aoi_ids):
        stored = {
            "a": CustomAoiGeometry("a", MultiPolygon([box(0, 0, 1, 1)])),
            "b": CustomAoiGeometry("b", MultiPolygon([box(0, 0, 2, 2)])),
        }
        return [stored[aoi_id] for aoi_id in aoi_ids if aoi_id in stored]


@pytest.fixture
def repository():
    return CustomAoiGeometryRepository(FakeCustomAoiRepository)


@pytest.mark.asyncio
async def test_geometries_follow_the_requested_order_with_geodesic_areas(repository):
    geometries, areas_ha = await repository.load(
        SavedCustomAreaOfInterest(ids=["b", "a"])
    )

    assert [geometry.area for geometry in geometries] == [4.0, 1.0]
    # a 1x1 degree cell at the equator is ~1,230,000 ha
    assert areas_ha[1] == pytest.approx(1_230_000, rel=0.01)
    assert areas_ha[0] > areas_ha[1]


@pytest.mark.asyncio
async def test_unknown_ids_are_not_found(repository):
    with pytest.raises(AoiNotFoundError, match="missing"):
        await repository.load(SavedCustomAreaOfInterest(ids=["a", "missing"]))
