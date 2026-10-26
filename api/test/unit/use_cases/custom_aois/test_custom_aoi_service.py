import pytest
from shapely.geometry import MultiPolygon, Polygon, box, mapping

from app.domain.models.custom_aoi import MonitorStatus
from app.models.custom_aois import CustomAoisIn
from app.use_cases.custom_aois.custom_aoi_service import (
    CustomAoiService,
    InvalidCustomAoiError,
)

BOWTIE = Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])


def custom_aois(*geometries_by_id):
    return CustomAoisIn(
        aois=[
            {
                "id": aoi_id,
                "geometry": mapping(geometry),
                "name": "My forest",
                "user_id": "user-1",
                "email": "owner@example.org",
                "monitor_status": "monitored",
            }
            for aoi_id, geometry in geometries_by_id
        ]
    )


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

    await CustomAoiService(repository).save(custom_aois(("a", polygon_z)))

    stored = repository.stored["a"]
    assert (stored.name, stored.user_id, stored.email, stored.monitor_status) == (
        "My forest",
        "user-1",
        "owner@example.org",
        MonitorStatus.monitored,
    )
    assert isinstance(stored.geometry, MultiPolygon)
    assert not stored.geometry.has_z
    # a 1x1 degree cell at the equator is ~1,230,000 ha
    assert stored.area_ha == pytest.approx(1_230_000, rel=0.01)


@pytest.mark.asyncio
async def test_invalid_geometries_are_repaired_before_saving():
    repository = FakeCustomAoiRepository()

    await CustomAoiService(repository).save(custom_aois(("a", BOWTIE)))

    assert repository.stored["a"].geometry.is_valid


@pytest.mark.asyncio
async def test_overlapping_parts_are_merged_so_area_is_not_double_counted():
    repository = FakeCustomAoiRepository()
    overlapping = MultiPolygon([box(0, 0, 2, 2), box(1, 1, 3, 3)])

    await CustomAoiService(repository).save(custom_aois(("a", overlapping)))

    stored = repository.stored["a"]
    assert stored.geometry.is_valid
    assert stored.geometry.area == pytest.approx(7.0)


@pytest.mark.asyncio
async def test_geometries_without_area_are_rejected_and_nothing_is_saved():
    repository = FakeCustomAoiRepository()
    line_shaped = Polygon([(0, 0), (1, 1), (2, 2), (0, 0)])

    with pytest.raises(InvalidCustomAoiError, match="b"):
        await CustomAoiService(repository).save(
            custom_aois(("a", box(0, 0, 1, 1)), ("b", line_shaped))
        )

    assert repository.stored == {}


@pytest.mark.asyncio
async def test_delete_removes_by_id():
    repository = FakeCustomAoiRepository()

    await CustomAoiService(repository).delete("a")

    assert repository.deleted == ["a"]
