from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.authentication import get_authenticator, require_resource_watch_admin
from app.models.common.authentication import User

_app = FastAPI()


@_app.get("/protected")
async def protected(user: User = Depends(require_resource_watch_admin)):
    return {"id": user.id, "role": user.role}


class FakeAuthenticator:
    """Maps a token to a User (or None for an invalid/expired token)."""

    def __init__(self, users):
        self.users = users

    async def get_user(self, token):
        return self.users.get(token)


def use(users):
    _app.dependency_overrides[get_authenticator] = lambda: FakeAuthenticator(users)


def teardown_function():
    _app.dependency_overrides.clear()


_client = TestClient(_app)


def test_missing_token_returns_401():
    use({})
    assert _client.get("/protected").status_code == 401


def test_invalid_token_returns_401():
    use({"bad": None})
    r = _client.get("/protected", headers={"Authorization": "Bearer bad"})
    assert r.status_code == 401


def test_non_admin_token_returns_403():
    use({"t": User(id="u", role="USER", extraUserData={})})
    r = _client.get("/protected", headers={"Authorization": "Bearer t"})
    assert r.status_code == 403


def test_admin_token_passes():
    use({"t": User(id="u", role="ADMIN", extraUserData={})})
    r = _client.get("/protected", headers={"Authorization": "Bearer t"})
    assert r.status_code == 200
    assert r.json()["role"] == "ADMIN"
