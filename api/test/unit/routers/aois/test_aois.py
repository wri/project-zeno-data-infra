import pytest
from fastapi.testclient import TestClient
from shapely.geometry import box

from app.authentication import get_authenticator
from app.dependencies import get_aoi_geometry_repository
from app.domain.repositories.aoi_geometry_repository import AoiNotFoundError
from app.main import app
from app.models.common.authentication import User

client = TestClient(app)
ADMIN = {"Authorization": "Bearer admin"}


class FakeAuthenticator:
    async def get_user(self, token):
        roles = {"admin": "ADMIN", "user": "USER"}
        if token not in roles:
            return None
        return User(id=token, role=roles[token], extraUserData={})


class FakeGeometryRepository:
    async def load(self, aoi):
        if aoi.ids != ["BRA.14"]:
            raise AoiNotFoundError(f"AOI ids not found: {aoi.ids}")
        return [box(0, 0, 1, 1)], [12.5]


@pytest.fixture(autouse=True)
def fakes():
    app.dependency_overrides[get_authenticator] = FakeAuthenticator
    app.dependency_overrides[get_aoi_geometry_repository] = FakeGeometryRepository
    yield
    app.dependency_overrides.clear()


@pytest.mark.parametrize(
    "headers, status", [({}, 401), ({"Authorization": "Bearer user"}, 403)]
)
def test_only_admins_can_get_an_aoi(headers, status):
    response = client.get("/v0/aois/admin/BRA.14", headers=headers)

    assert response.status_code == status


def test_aoi_geometry_is_returned_as_a_geojson_feature():
    response = client.get("/v0/aois/admin/BRA.14", headers=ADMIN)

    assert response.status_code == 200
    feature = response.json()["data"]
    assert (feature["type"], feature["id"]) == ("Feature", "BRA.14")
    assert feature["geometry"]["type"] == "Polygon"
    assert feature["properties"] == {"aoi_type": "admin", "area_ha": 12.5}


def test_unknown_aoi_is_not_found():
    response = client.get("/v0/aois/admin/BRA.99", headers=ADMIN)

    assert response.status_code == 404


def test_custom_aois_are_not_yet_supported():
    response = client.get("/v0/aois/custom_aoi/home", headers=ADMIN)

    assert response.status_code == 422
