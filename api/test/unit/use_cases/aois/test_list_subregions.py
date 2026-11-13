import pytest
from shapely.geometry import box

from app.domain.models.aoi_subregion import AoiSubregion
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
)
from app.use_cases.aois.list_subregions import (
    ListSubregions,
    UnsupportedSubregionsError,
)


class FakeAdminSubregions:
    def __init__(self):
        self.requests = []

    async def within(self, parent_id, level):
        self.requests.append((parent_id, level))
        return [
            AoiSubregion("admin", "IDN.1", "Aceh", 10.0),
            AoiSubregion("admin", "IDN.2", "Bali", 30.0),
            AoiSubregion("admin", "IDN.3", "Banten", 20.0),
        ]


class FakeIntersectingSubregions:
    def __init__(self):
        self.geometries = []

    async def within(self, geometry):
        self.geometries.append(geometry)
        return [AoiSubregion("protected_area", "1", "Teide", 5.0)]


class FakeGeometries:
    async def load(self, aoi):
        return [box(0, 0, 1, 1)], [1.0]


def list_subregions(admin=None, intersecting=None):
    return ListSubregions(
        admin=admin or FakeAdminSubregions(),
        intersecting={"protected_area": intersecting or FakeIntersectingSubregions()},
        geometries=FakeGeometries(),
    )


@pytest.mark.asyncio
async def test_admin_subregions_default_to_the_next_level_largest_first():
    admin = FakeAdminSubregions()

    result = await list_subregions(admin=admin).within(
        AdminAreaOfInterest(ids=["IDN"]), "admin", level=None, limit=2
    )

    assert admin.requests == [("IDN", 1)]
    assert result.total == 3
    assert [subregion.name for subregion in result.subregions] == ["Bali", "Banten"]


@pytest.mark.asyncio
async def test_the_global_area_lists_countries():
    admin = FakeAdminSubregions()

    await list_subregions(admin=admin).within(
        AdminAreaOfInterest(ids=["global"]), "admin", level=None, limit=50
    )

    assert admin.requests == [(None, 0)]


@pytest.mark.asyncio
async def test_other_area_types_are_found_inside_the_parent_geometry():
    intersecting = FakeIntersectingSubregions()

    result = await list_subregions(intersecting=intersecting).within(
        AdminAreaOfInterest(ids=["ESP.7.2"]), "protected_area", level=None, limit=50
    )

    assert intersecting.geometries == [box(0, 0, 1, 1)]
    assert [subregion.name for subregion in result.subregions] == ["Teide"]


@pytest.mark.asyncio
async def test_admin_subregions_of_a_non_admin_area_are_not_supported():
    with pytest.raises(UnsupportedSubregionsError):
        await list_subregions().within(
            ProtectedAreaOfInterest(ids=["1"]), "admin", level=None, limit=50
        )


@pytest.mark.asyncio
async def test_a_level_above_the_parent_is_rejected():
    with pytest.raises(UnsupportedSubregionsError):
        await list_subregions().within(
            AdminAreaOfInterest(ids=["IDN.1"]), "admin", level=1, limit=50
        )
