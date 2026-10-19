import pytest
from shapely.geometry import MultiPolygon, Polygon, box, mapping

from app.use_cases.custom_areas.custom_area_service import (
    CustomAreaService,
    InvalidCustomAreaError,
)

BOWTIE = Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])


class FakeCustomAoiRepository:
    def __init__(self):
        self.stored = {}
        self.deleted = []

    async def upsert(self, aois):
        self.stored.update({aoi.aoi_id: aoi for aoi in aois})

    async def delete(self, aoi_id):
        self.deleted.append(aoi_id)


@pytest.mark.asyncio
async def test_saved_areas_are_2d_multipolygons_with_geodesic_area():
    repository = FakeCustomAoiRepository()
    polygon_z = Polygon([(0, 0, 5), (1, 0, 5), (1, 1, 5), (0, 1, 5), (0, 0, 5)])

    await CustomAreaService(repository).save({"a": mapping(polygon_z)})

    stored = repository.stored["a"]
    assert isinstance(stored.geometry, MultiPolygon)
    assert not stored.geometry.has_z
    # a 1x1 degree cell at the equator is ~1,230,000 ha
    assert stored.area_ha == pytest.approx(1_230_000, rel=0.01)


@pytest.mark.asyncio
async def test_invalid_geometries_are_repaired_before_saving():
    repository = FakeCustomAoiRepository()

    await CustomAreaService(repository).save({"a": mapping(BOWTIE)})

    assert repository.stored["a"].geometry.is_valid


@pytest.mark.asyncio
async def test_geometries_without_area_are_rejected_and_nothing_is_saved():
    repository = FakeCustomAoiRepository()
    line_shaped = Polygon([(0, 0), (1, 1), (2, 2), (0, 0)])

    with pytest.raises(InvalidCustomAreaError, match="b"):
        await CustomAreaService(repository).save(
            {"a": mapping(box(0, 0, 1, 1)), "b": mapping(line_shaped)}
        )

    assert repository.stored == {}


@pytest.mark.asyncio
async def test_delete_removes_by_id():
    repository = FakeCustomAoiRepository()

    await CustomAreaService(repository).delete("a")

    assert repository.deleted == ["a"]
