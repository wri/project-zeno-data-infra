import pytest
from fastapi.testclient import TestClient
from shapely.geometry import box

from app.authentication import get_authenticator
from app.dependencies import get_aoi_geometry_repository, get_aoi_name_index
from app.domain.models.aoi_match import AoiMatch
from app.domain.models.custom_aoi import CustomAoiName
from app.domain.repositories.aoi_geometry_repository import AoiNotFoundError
from app.main import app
from app.models.common.authentication import User
from app.routers.aois import get_custom_aoi_repository_factory

client = TestClient(app)
ADMIN = {"Authorization": "Bearer admin"}


class FakeAuthenticator:
    async def get_user(self, token):
        roles = {"admin": "ADMIN", "user": "USER"}
        if token not in roles:
            return None
        return User(id=token, role=roles[token], extraUserData={})


class FakeNameIndex:
    def __init__(self):
        self.searches = []

    async def search(self, text, aoi_types, limit, custom_aois=()):
        self.searches.append((text, aoi_types, limit, list(custom_aois)))
        return [AoiMatch("admin", "BRA.14", "Pará", "Pará, Brazil")]


class FakeCustomAoiRepository:
    async def names_owned_by(self, user_id):
        return [CustomAoiName("home", "Home farm")] if user_id == "user-1" else []


class FakeGeometryRepository:
    async def load(self, aoi):
        if aoi.ids != ["BRA.14"] and aoi.ids != ["home"]:
            raise AoiNotFoundError(f"AOI ids not found: {aoi.ids}")
        return [box(0, 0, 1, 1)], [12.5]


@pytest.fixture
def name_index():
    index = FakeNameIndex()
    app.dependency_overrides[get_authenticator] = FakeAuthenticator
    app.dependency_overrides[get_aoi_name_index] = lambda: index
    app.dependency_overrides[get_custom_aoi_repository_factory] = lambda: (
        FakeCustomAoiRepository
    )
    app.dependency_overrides[get_aoi_geometry_repository] = FakeGeometryRepository
    yield index
    app.dependency_overrides.clear()


@pytest.mark.parametrize(
    "headers, status", [({}, 401), ({"Authorization": "Bearer user"}, 403)]
)
def test_both_endpoints_require_an_admin(name_index, headers, status):
    search = client.get("/v0/aois/search", params={"q": "para"}, headers=headers)
    geometry = client.get("/v0/aois/admin/BRA.14", headers=headers)

    assert (search.status_code, geometry.status_code) == (status, status)


def test_search_returns_matching_aois(name_index):
    response = client.get(
        "/v0/aois/search",
        params={"q": "para", "type": "admin", "limit": 5},
        headers=ADMIN,
    )

    assert response.status_code == 200
    assert response.json()["data"] == [
        {
            "aoi_type": "admin",
            "aoi_id": "BRA.14",
            "name": "Pará",
            "display_name": "Pará, Brazil",
        }
    ]
    assert name_index.searches == [("para", ["admin"], 5, [])]


def test_long_search_text_is_rejected_before_searching(name_index):
    response = client.get("/v0/aois/search", params={"q": "a" * 101}, headers=ADMIN)

    assert response.status_code == 422
    assert name_index.searches == []


def test_search_includes_the_users_own_custom_aois(name_index):
    client.get(
        "/v0/aois/search", params={"q": "home", "user_id": "user-1"}, headers=ADMIN
    )

    assert name_index.searches[0][3] == [CustomAoiName("home", "Home farm")]


@pytest.mark.parametrize(
    "aoi_type, aoi_id", [("admin", "BRA.14"), ("custom_aoi", "home")]
)
def test_aoi_geometry_is_returned_as_a_geojson_feature(name_index, aoi_type, aoi_id):
    response = client.get(f"/v0/aois/{aoi_type}/{aoi_id}", headers=ADMIN)

    assert response.status_code == 200
    feature = response.json()["data"]
    assert (feature["type"], feature["id"]) == ("Feature", aoi_id)
    assert feature["geometry"]["type"] == "Polygon"
    assert feature["properties"] == {"aoi_type": aoi_type, "area_ha": 12.5}


def test_unknown_aoi_is_not_found(name_index):
    response = client.get("/v0/aois/admin/BRA.99", headers=ADMIN)

    assert response.status_code == 404
