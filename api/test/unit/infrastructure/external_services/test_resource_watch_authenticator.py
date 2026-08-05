import httpx
import pytest

from app.infrastructure.external_services.resource_watch_authenticator import (
    ResourceWatchAuthenticator,
    TokenValidationCache,
    UpstreamAuthError,
)


def build_authenticator(handler, cache=None):
    """Authenticator wired to an in-memory transport standing in for the RW API."""
    transport = httpx.MockTransport(handler)
    return ResourceWatchAuthenticator(
        api_url="https://rw.test",
        client_factory=lambda: httpx.AsyncClient(transport=transport),
        cache=cache,
    )


class FakeClock:
    """Deterministic monotonic clock so TTL tests don't sleep."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def counting_handler(response_factory):
    """A MockTransport handler that counts how many times RW was actually hit."""
    calls = {"count": 0}

    def handler(request):
        calls["count"] += 1
        return response_factory()

    return handler, calls


def admin_response():
    return httpx.Response(200, json={"id": "u1", "role": "ADMIN", "extraUserData": {}})


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


@pytest.mark.asyncio
async def test_valid_token_is_cached_within_ttl():
    cache = TokenValidationCache(ttl_seconds=120, clock=FakeClock())
    handler, calls = counting_handler(admin_response)
    authenticator = build_authenticator(handler, cache=cache)

    first = await authenticator.get_user("t")
    second = await authenticator.get_user("t")

    assert first.role == "ADMIN" and second.role == "ADMIN"
    assert calls["count"] == 1  # RW hit once, second call served from cache


@pytest.mark.asyncio
async def test_invalid_token_result_is_cached():
    # negative results are cached too, so an invalid-token flood can't hammer RW
    cache = TokenValidationCache(ttl_seconds=120, clock=FakeClock())
    handler, calls = counting_handler(lambda: httpx.Response(401))
    authenticator = build_authenticator(handler, cache=cache)

    assert await authenticator.get_user("bad") is None
    assert await authenticator.get_user("bad") is None
    assert calls["count"] == 1


@pytest.mark.asyncio
async def test_cache_expires_after_ttl():
    clock = FakeClock()
    cache = TokenValidationCache(ttl_seconds=120, clock=clock)
    handler, calls = counting_handler(admin_response)
    authenticator = build_authenticator(handler, cache=cache)

    await authenticator.get_user("t")
    clock.advance(121)
    await authenticator.get_user("t")

    assert calls["count"] == 2  # re-queried after the entry expired


@pytest.mark.asyncio
async def test_upstream_error_is_not_cached():
    # a failed decision must not stick; the next call should retry
    responses = [httpx.Response(500), admin_response()]
    authenticator = build_authenticator(
        lambda request: responses.pop(0), cache=TokenValidationCache()
    )

    with pytest.raises(UpstreamAuthError):
        await authenticator.get_user("t")
    assert (await authenticator.get_user("t")).role == "ADMIN"
