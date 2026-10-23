import pytest
from shapely.geometry import box

from app.infrastructure.external_services.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.models.common.areas_of_interest import (
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)


class FakeDataApi:
    """Answers queries with the given rows, in its own order."""

    def __init__(self, rows):
        self.rows = rows
        self.requests = []

    async def __call__(self, url, params):
        self.requests.append((url, params))
        return {"data": self.rows, "status": "success"}


def row(id_column, aoi_id, size):
    return {
        id_column: aoi_id,
        "geom": box(0, 0, size, size).wkb_hex,
        "gfw_area__ha": size,
    }


@pytest.mark.asyncio
async def test_geometries_are_returned_in_requested_order():
    data_api = FakeDataApi([row("sitrecid", 100, 3), row("sitrecid", 25, 1)])
    repository = DataApiAoiGeometryRepository(send_request=data_api)

    geometries, areas_ha = await repository.load(
        KeyBiodiversityAreaOfInterest(ids=["25", "100"])
    )

    assert [geometry.area for geometry in geometries] == [1.0, 9.0]
    assert areas_ha == [1, 3]


@pytest.mark.asyncio
async def test_quotes_in_ids_are_escaped():
    data_api = FakeDataApi([row("site_pid", "1'2", 1)])
    repository = DataApiAoiGeometryRepository(send_request=data_api)

    await repository.load(ProtectedAreaOfInterest(ids=["1'2"]))

    _, params = data_api.requests[0]
    assert "in ('1''2')" in params["sql"]


@pytest.mark.asyncio
async def test_unknown_ids_raise():
    repository = DataApiAoiGeometryRepository(send_request=FakeDataApi([]))

    with pytest.raises(ValueError, match="99"):
        await repository.load(ProtectedAreaOfInterest(ids=["99"]))
