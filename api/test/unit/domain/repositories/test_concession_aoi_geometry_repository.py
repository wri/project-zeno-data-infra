import pytest

from app.domain.repositories.concession_aoi_geometry_repository import (
    ConcessionAoiGeometryRepository,
)
from app.models.common.areas_of_interest import ConcessionAreaOfInterest


class NamedGeometryRepository:
    def __init__(self, name):
        self.name = name

    async def load(self, aoi):
        return [self.name], [0.0]


@pytest.mark.asyncio
async def test_loads_from_repository_registered_for_concession_type():
    repository = ConcessionAoiGeometryRepository(
        {"oil_palm": NamedGeometryRepository("oil_palm")}
    )

    geometries, _ = await repository.load(
        ConcessionAreaOfInterest(concession_type="oil_palm", ids=["1"])
    )

    assert geometries == ["oil_palm"]
