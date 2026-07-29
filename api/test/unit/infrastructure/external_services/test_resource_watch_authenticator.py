import httpx
import pytest

from app.infrastructure.external_services.resource_watch_authenticator import (
    ResourceWatchAuthenticator,
    UpstreamAuthError,
)


def build_authenticator(handler):
    """Authenticator wired to an in-memory transport standing in for the RW API."""
    transport = httpx.MockTransport(handler)
    return ResourceWatchAuthenticator(
        api_url="https://rw.test",
        client_factory=lambda: httpx.AsyncClient(transport=transport),
    )


@pytest.mark.asyncio
async def test_returns_user_for_valid_token():
    def handler(request):
        # sends the token as a bearer credential to the check-logged endpoint
        assert request.headers["Authorization"] == "Bearer good-token"
        assert request.url.path == "/auth/check-logged"
        return httpx.Response(
            200,
            json={"id": "u1", "role": "ADMIN", "extraUserData": {"apps": ["rw"]}},
        )

    user = await build_authenticator(handler).get_user("good-token")

    assert user is not None
    assert user.id == "u1"
    assert user.role == "ADMIN"


@pytest.mark.asyncio
async def test_returns_none_for_unauthorized_token():
    def handler(request):
        return httpx.Response(401, json={"errors": [{"detail": "Unauthorized"}]})

    assert await build_authenticator(handler).get_user("bad-token") is None


@pytest.mark.asyncio
async def test_raises_on_upstream_failure():
    # anything other than 200/401 means we cannot make an auth decision
    def handler(request):
        return httpx.Response(500, text="boom")

    with pytest.raises(UpstreamAuthError):
        await build_authenticator(handler).get_user("whatever")
