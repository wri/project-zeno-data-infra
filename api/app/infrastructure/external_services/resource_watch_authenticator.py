import time
from typing import Callable, Dict, Optional, Tuple

import httpx

from app.models.common.authentication import User


class UpstreamAuthError(Exception):
    """The authorization server could not be reached or gave an unusable
    response, so no authentication decision can be made."""


class TokenValidationCache:
    """A short-lived TTL cache of token -> resolved identity.

    Caches negative results (``None``) too, so a flood of invalid tokens cannot
    hammer the authorization server. The TTL bounds how long a revoked token
    keeps working, so keep it short.
    """

    def __init__(
        self,
        ttl_seconds: float = 120.0,
        max_size: int = 1024,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self.clock = clock
        self.entries: Dict[str, Tuple[float, Optional[User]]] = {}

    def get(self, token: str) -> Tuple[bool, Optional[User]]:
        """Return ``(hit, user)``. ``hit`` distinguishes a cached ``None`` from a
        miss."""
        entry = self.entries.get(token)
        if entry is None:
            return False, None
        expires_at, user = entry
        if self.clock() >= expires_at:
            del self.entries[token]
            return False, None
        return True, user

    def set(self, token: str, user: Optional[User]) -> None:
        if len(self.entries) >= self.max_size:
            self.entries.clear()  # crude but bounded; avoids unbounded growth
        self.entries[token] = (self.clock() + self.ttl_seconds, user)


class ResourceWatchAuthenticator:
    """Validates a ResourceWatch bearer token against the RW API.

    Mirrors gfw-data-api's ``who_am_i``: a GET to ``/auth/check-logged`` with the
    token as a bearer credential. Pure infrastructure -- it resolves identity, it
    does not decide authorization (that policy lives at the edge).
    """

    def __init__(
        self,
        api_url: str,
        timeout_seconds: float = 10.0,
        client_factory: Callable[[], httpx.AsyncClient] = httpx.AsyncClient,
        cache: Optional[TokenValidationCache] = None,
    ):
        self.api_url = api_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.client_factory = client_factory
        self.cache = cache

    async def get_user(self, token: str) -> Optional[User]:
        """Return the authenticated user, or ``None`` if the token is invalid or
        expired. Raises ``UpstreamAuthError`` if the RW API is unreachable or
        returns an unexpected status.

        Successful decisions (a user or ``None``) are cached; upstream failures
        are not, so a transient RW blip does not stick."""
        if self.cache is not None:
            hit, cached_user = self.cache.get(token)
            if hit:
                return cached_user

        user = await self._fetch_user(token)

        if self.cache is not None:
            self.cache.set(token, user)
        return user

    async def _fetch_user(self, token: str) -> Optional[User]:
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{self.api_url}/auth/check-logged"

        try:
            async with self.client_factory() as client:
                response = await client.get(
                    url, headers=headers, timeout=self.timeout_seconds
                )
        except httpx.HTTPError as error:
            raise UpstreamAuthError(
                "Call to the authorization server failed"
            ) from error

        if response.status_code == 401:
            return None
        if response.status_code != 200:
            raise UpstreamAuthError(
                "Authorization server responded with " f"status {response.status_code}"
            )

        return User(**response.json())
