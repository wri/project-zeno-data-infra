from typing import Callable, Optional

import httpx

from app.models.common.authentication import User


class UpstreamAuthError(Exception):
    """The authorization server could not be reached or gave an unusable
    response, so no authentication decision can be made."""


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
    ):
        self.api_url = api_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.client_factory = client_factory

    async def get_user(self, token: str) -> Optional[User]:
        """Return the authenticated user, or ``None`` if the token is invalid or
        expired. Raises ``UpstreamAuthError`` if the RW API is unreachable or
        returns an unexpected status."""
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
