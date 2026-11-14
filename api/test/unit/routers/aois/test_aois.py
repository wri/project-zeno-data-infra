import pytest
from fastapi.testclient import TestClient
from shapely.geometry import box

from app.authentication import get_authenticator
from app.dependencies import (
    get_aoi_geometry_repository,
    get_aoi_name_search,
    get_list_subregions,
)
from app.domain.models.aoi_match import AoiMatch
from app.domain.models.aoi_subregion import AoiSubregion
from app.domain.models.custom_aoi import CustomAoiName
from app.domain.repositories.aoi_geometry_repository import AoiNotFoundError
from app.main import app
from app.models.common.authentication import User
from app.routers.aois import get_custom_aoi_repository_factory
from app.use_cases.aois.list_subregions import Subregions

client = TestClient(app)
ADMIN = {"Authorization": "Bearer admin"}


class FakeAuthenticator:
    async def get_user(self, token):
        roles = {"admin": "ADMIN", "user": "USER"}
        if token not in roles:
            return None
        return User(id=token, role=roles[token], extraUserData={})


class FakeNameSearch:
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
        if aoi.ids != ["BRA.14"]:
            raise AoiNotFoundError(f"AOI ids not found: {aoi.ids}")
        return [box(0, 0, 1, 1)], [12.5]


class FakeListSubregions:
    def __init__(self):
        self.requests = []

    async def within(self, parent, area_type, level, limit):
        if parent.ids != ["IDN"]:
            raise AoiNotFoundError(f"AOI ids not found: {parent.ids}")
        self.requests.append((parent.type, parent.ids, area_type, level, limit))
        return Subregions(
            total=34, subregions=[AoiSubregion("admin", "IDN.24", "Papua", 3.2e7)]
        )


@pytest.fixture
def name_index():
    index = FakeNameSearch()
    app.dependency_overrides[get_authenticator] = FakeAuthenticator
    app.dependency_overrides[get_aoi_name_search] = lambda: index
    app.dependency_overrides[get_custom_aoi_repository_factory] = lambda: (
        FakeCustomAoiRepository
    )
    app.dependency_overrides[get_aoi_geometry_repository] = FakeGeometryRepository
    app.dependency_overrides[get_list_subregions] = FakeListSubregions
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


def test_aoi_geometry_is_returned_as_a_geojson_feature(name_index):
    response = client.get("/v0/aois/admin/BRA.14", headers=ADMIN)

    assert response.status_code == 200
    feature = response.json()["data"]
    assert (feature["type"], feature["id"]) == ("Feature", "BRA.14")
    assert feature["geometry"]["type"] == "Polygon"
    assert feature["properties"] == {"aoi_type": "admin", "area_ha": 12.5}


def test_unknown_aoi_is_not_found(name_index):
    response = client.get("/v0/aois/admin/BRA.99", headers=ADMIN)

    assert response.status_code == 404


def test_custom_aois_are_not_yet_supported(name_index):
    response = client.get("/v0/aois/custom_aoi/home", headers=ADMIN)

    assert response.status_code == 422


def test_search_can_be_limited_to_custom_aois(name_index):
    response = client.get(
        "/v0/aois/search", params={"q": "home", "type": "custom_aoi"}, headers=ADMIN
    )

    assert response.status_code == 200
    assert name_index.searches[0][1] == ["custom_aoi"]


def test_subregions_are_listed_with_their_total(name_index):
    response = client.get(
        "/v0/aois/admin/IDN/subregions",
        params={"type": "admin", "level": 1, "limit": 1},
        headers=ADMIN,
    )

    assert response.status_code == 200
    assert response.json()["total"] == 34
    assert response.json()["data"] == [
        {"aoi_type": "admin", "aoi_id": "IDN.24", "name": "Papua"}
    ]


def test_subregions_of_an_unknown_aoi_are_not_found(name_index):
    response = client.get("/v0/aois/admin/XYZ/subregions", headers=ADMIN)

    assert response.status_code == 404
