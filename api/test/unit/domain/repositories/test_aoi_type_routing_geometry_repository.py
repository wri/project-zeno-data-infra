import pytest

from app.domain.repositories.aoi_type_routing_geometry_repository import (
    AoiTypeRoutingGeometryRepository,
)
from app.models.common.areas_of_interest import (
    ConcessionAreaOfInterest,
    ProtectedAreaOfInterest,
)


class NamedGeometryRepository:
    def __init__(self, name):
        self.name = name

    async def load(self, aoi):
        return [self.name], [0.0]


@pytest.mark.asyncio
async def test_loads_from_repository_registered_for_aoi_type():
    repository = AoiTypeRoutingGeometryRepository(
        {
            "protected_area": NamedGeometryRepository("data_api"),
            "concession": NamedGeometryRepository("geoparquet"),
        }
    )

    geometries, _ = await repository.load(
        ConcessionAreaOfInterest(concession_type="oil_palm", ids=["1"])
    )

    assert geometries == ["geoparquet"]


@pytest.mark.asyncio
async def test_unregistered_aoi_type_raises():
    repository = AoiTypeRoutingGeometryRepository({})

    with pytest.raises(ValueError, match="protected_area"):
        await repository.load(ProtectedAreaOfInterest(ids=["1"]))
