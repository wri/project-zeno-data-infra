from fastapi.testclient import TestClient
from shapely.geometry import box

from app.dependencies import get_aoi_geometry_repository, get_aoi_name_index
from app.domain.models.aoi_match import AoiMatch
from app.domain.repositories.aoi_geometry_repository import AoiNotFoundError
from app.main import app

client = TestClient(app)


class FakeNameIndex:
    def __init__(self):
        self.searches = []

    async def search(self, text, aoi_types, limit):
        self.searches.append((text, aoi_types, limit))
        return [AoiMatch("admin", "BRA.14", "Pará", "Pará, Brazil")]


class FakeGeometryRepository:
    async def load(self, aoi):
        if aoi.ids != ["BRA.14"]:
            raise AoiNotFoundError(f"AOI ids not found: {aoi.ids}")
        return [box(0, 0, 1, 1)], [12.5]


def test_search_returns_matching_aois():
    name_index = FakeNameIndex()
    app.dependency_overrides[get_aoi_name_index] = lambda: name_index
    try:
        response = client.get(
            "/v0/aois/search", params={"q": "para", "type": "admin", "limit": 5}
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["data"] == [
        {
            "aoi_type": "admin",
            "aoi_id": "BRA.14",
            "name": "Pará",
            "display_name": "Pará, Brazil",
        }
    ]
    assert name_index.searches == [("para", ["admin"], 5)]


def test_aoi_geometry_is_returned_as_a_geojson_feature():
    app.dependency_overrides[get_aoi_geometry_repository] = FakeGeometryRepository
    try:
        response = client.get("/v0/aois/admin/BRA.14")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    feature = response.json()["data"]
    assert feature["type"] == "Feature"
    assert feature["id"] == "BRA.14"
    assert feature["geometry"]["type"] == "Polygon"
    assert feature["properties"] == {"aoi_type": "admin", "area_ha": 12.5}


def test_unknown_aoi_is_not_found():
    app.dependency_overrides[get_aoi_geometry_repository] = FakeGeometryRepository
    try:
        response = client.get("/v0/aois/admin/BRA.99")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
