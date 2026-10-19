import pytest
from fastapi.testclient import TestClient
from shapely.geometry import box, mapping

from app.authentication import get_authenticator
from app.main import app
from app.models.common.authentication import User
from app.routers.custom_areas import get_custom_area_service

ADMIN = {"Authorization": "Bearer admin"}


class FakeAuthenticator:
    async def get_user(self, token):
        roles = {"admin": "ADMIN", "user": "USER"}
        return (
            User(id=token, role=roles[token], extraUserData={})
            if token in roles
            else None
        )


class RecordingCustomAreaService:
    def __init__(self):
        self.saved = None
        self.deleted = None

    async def save(self, geometries):
        self.saved = geometries

    async def delete(self, aoi_id):
        self.deleted = aoi_id


@pytest.fixture
def service():
    recording = RecordingCustomAreaService()
    app.dependency_overrides[get_authenticator] = FakeAuthenticator
    app.dependency_overrides[get_custom_area_service] = lambda: recording
    yield recording
    app.dependency_overrides.clear()


client = TestClient(app)


def feature_collection(*ids, geometry=None):
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": aoi_id,
                "geometry": geometry or mapping(box(0, 0, 1, 1)),
                "properties": {},
            }
            for aoi_id in ids
        ],
    }


def test_put_saves_geometries_by_id(service):
    response = client.put(
        "/v0/custom_areas", json=feature_collection("a", "b"), headers=ADMIN
    )

    assert response.status_code == 200
    assert response.json()["data"]["ids"] == ["a", "b"]
    assert sorted(service.saved) == ["a", "b"]


def test_delete_removes_by_id(service):
    response = client.delete("/v0/custom_areas/a", headers=ADMIN)

    assert response.status_code == 204
    assert service.deleted == "a"


@pytest.mark.parametrize(
    "headers, status", [({}, 401), ({"Authorization": "Bearer user"}, 403)]
)
def test_writes_require_an_admin(service, headers, status):
    assert (
        client.put(
            "/v0/custom_areas", json=feature_collection("a"), headers=headers
        ).status_code
        == status
    )
    assert client.delete("/v0/custom_areas/a", headers=headers).status_code == status
    assert service.saved is None and service.deleted is None


@pytest.mark.parametrize(
    "body",
    [
        feature_collection("a", "a"),
        feature_collection(*[str(i) for i in range(501)]),
        feature_collection("a", geometry={"type": "Point", "coordinates": [0, 0]}),
        {
            "type": "FeatureCollection",
            "features": [{"type": "Feature", "geometry": mapping(box(0, 0, 1, 1))}],
        },
    ],
    ids=["duplicate ids", "over 500 features", "not a polygon", "missing id"],
)
def test_invalid_requests_are_rejected(service, body):
    response = client.put("/v0/custom_areas", json=body, headers=ADMIN)

    assert response.status_code == 422
    assert service.saved is None
