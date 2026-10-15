import pytest

from app.domain.repositories.fallback_aoi_geometry_repository import (
    FallbackAoiGeometryRepository,
)
from app.models.common.areas_of_interest import ProtectedAreaOfInterest

AOI = ProtectedAreaOfInterest(ids=["1"])


class NamedGeometryRepository:
    def __init__(self, name):
        self.name = name

    async def load(self, aoi):
        return [self.name], [0.0]


class FailingGeometryRepository:
    def __init__(self, error):
        self.error = error

    async def load(self, aoi):
        raise self.error


@pytest.mark.asyncio
async def test_primary_is_used_when_available():
    repository = FallbackAoiGeometryRepository(
        NamedGeometryRepository("geoparquet"), NamedGeometryRepository("data_api")
    )

    assert (await repository.load(AOI))[0] == ["geoparquet"]


@pytest.mark.asyncio
async def test_fallback_is_used_when_primary_dataset_is_missing():
    repository = FallbackAoiGeometryRepository(
        FailingGeometryRepository(FileNotFoundError("aois.parquet")),
        NamedGeometryRepository("data_api"),
    )

    assert (await repository.load(AOI))[0] == ["data_api"]


@pytest.mark.asyncio
async def test_other_primary_errors_are_not_masked_by_the_fallback():
    repository = FallbackAoiGeometryRepository(
        FailingGeometryRepository(ValueError("AOI ids not found: ['1']")),
        NamedGeometryRepository("data_api"),
    )

    with pytest.raises(ValueError, match="not found"):
        await repository.load(AOI)
