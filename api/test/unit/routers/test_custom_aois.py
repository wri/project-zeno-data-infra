import numpy as np
import pytest
from fastapi.testclient import TestClient
from shapely.geometry import Polygon, box, mapping

from app.authentication import get_authenticator
from app.domain.models.custom_aoi import MonitorStatus
from app.main import app
from app.models.common.authentication import User
from app.routers.custom_aois import get_custom_aoi_service

ADMIN = {"Authorization": "Bearer admin"}


class FakeAuthenticator:
    async def get_user(self, token):
        roles = {"admin": "ADMIN", "user": "USER"}
        return (
            User(id=token, role=roles[token], extraUserData={})
            if token in roles
            else None
        )


class RecordingCustomAoiService:
    def __init__(self):
        self.saved = None
        self.deleted = None

    async def save(self, custom_aois):
        self.saved = custom_aois

    async def delete(self, aoi_id):
        self.deleted = aoi_id


@pytest.fixture
def service():
    recording = RecordingCustomAoiService()
    app.dependency_overrides[get_authenticator] = FakeAuthenticator
    app.dependency_overrides[get_custom_aoi_service] = lambda: recording
    yield recording
    app.dependency_overrides.clear()


client = TestClient(app)


def circle(coordinates):
    angles = np.linspace(0, 2 * np.pi, coordinates)
    return Polygon(np.column_stack([np.cos(angles), np.sin(angles)]))


OWNER = {"name": "My forest", "user_id": "user-1"}


def custom_aois(*ids, geometry=None, **fields):
    details = fields or OWNER
    return {
        "aois": [
            {"id": aoi_id, "geometry": geometry or mapping(box(0, 0, 1, 1)), **details}
            for aoi_id in ids
        ]
    }


def test_put_saves_each_aoi_with_its_owner_name_and_monitor_status(service):
    body = custom_aois("a", "b")
    body["aois"][1].update(name="Watched", user_id="user-2", monitor_status="monitored")

    response = client.put("/v0/custom_aois", json=body, headers=ADMIN)

    assert response.status_code == 200
    assert response.json()["data"]["ids"] == ["a", "b"]
    assert [
        (aoi.id, aoi.name, aoi.user_id, aoi.monitor_status)
        for aoi in service.saved.aois
    ] == [
        ("a", "My forest", "user-1", MonitorStatus.not_monitored),
        ("b", "Watched", "user-2", MonitorStatus.monitored),
    ]


def test_delete_removes_by_id(service):
    response = client.delete("/v0/custom_aois/a", headers=ADMIN)

    assert response.status_code == 204
    assert service.deleted == "a"


@pytest.mark.parametrize(
    "headers, status", [({}, 401), ({"Authorization": "Bearer user"}, 403)]
)
def test_writes_require_an_admin(service, headers, status):
    assert (
        client.put(
            "/v0/custom_aois", json=custom_aois("a"), headers=headers
        ).status_code
        == status
    )
    assert client.delete("/v0/custom_aois/a", headers=headers).status_code == status
    assert service.saved is None and service.deleted is None


@pytest.mark.parametrize(
    "body",
    [
        custom_aois("a", "a"),
        custom_aois(*[str(i) for i in range(501)]),
        custom_aois("a", geometry={"type": "Point", "coordinates": [0, 0]}),
        {"aois": [{"geometry": mapping(box(0, 0, 1, 1)), **OWNER}]},
        custom_aois("a", geometry=mapping(circle(coordinates=100_002))),
        custom_aois("a", name="My forest"),
        custom_aois("a", user_id="user-1"),
        custom_aois("a", **OWNER, monitor_status="on"),
    ],
    ids=[
        "duplicate ids",
        "over 500 AOIs",
        "not a polygon",
        "missing id",
        "over 100,000 coordinates",
        "missing user id",
        "missing name",
        "unknown monitor status",
    ],
)
def test_invalid_requests_are_rejected(service, body):
    response = client.put("/v0/custom_aois", json=body, headers=ADMIN)

    assert response.status_code == 422
    assert service.saved is None
