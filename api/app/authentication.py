import os

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from app.infrastructure.external_services.resource_watch_authenticator import (
    ResourceWatchAuthenticator,
    TokenValidationCache,
    UpstreamAuthError,
)
from app.models.common.authentication import User

bearer_scheme = HTTPBearer(auto_error=False)

# Shared across the per-request authenticators built by get_authenticator, so a
# token is validated against RW at most once per TTL rather than on every request
# and every LRO poll.
token_validation_cache = TokenValidationCache()


def get_authenticator() -> ResourceWatchAuthenticator:
    """Edge-injected authenticator. Reads the RW API base from the environment so
    it can be pointed at staging without code changes; defaults to production."""
    return ResourceWatchAuthenticator(
        api_url=os.getenv("RW_API_URL", "https://api.resourcewatch.org"),
        cache=token_validation_cache,
    )


async def require_resource_watch_admin(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    authenticator: ResourceWatchAuthenticator = Depends(get_authenticator),
) -> User:
    """Guard that only lets ResourceWatch admins through.

    401 when the token is missing or invalid; 403 when the token is valid but the
    user is not an admin.
    """
    token = credentials.credentials if credentials else None
    return await resource_watch_admin(token, authenticator)


async def resource_watch_admin(
    token: str | None, authenticator: ResourceWatchAuthenticator
) -> User:
    if token is None:
        raise HTTPException(
            status_code=401,
            detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        user = await authenticator.get_user(token)
    except UpstreamAuthError:
        raise HTTPException(
            status_code=502, detail="Could not reach the authorization server"
        )

    if user is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if user.role != "ADMIN":
        raise HTTPException(
            status_code=403, detail="Requires a ResourceWatch admin account"
        )

    return user


class ResourceWatchAdminOnly:
    """The same guard for an application mounted outside FastAPI's dependency
    injection, such as the MCP server."""

    def __init__(self, app: ASGIApp, authenticator: ResourceWatchAuthenticator):
        self.app = app
        self.authenticator = authenticator

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            try:
                await resource_watch_admin(bearer_token(scope), self.authenticator)
            except HTTPException as error:
                response = JSONResponse(
                    {"detail": error.detail}, error.status_code, error.headers
                )
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


def bearer_token(scope: Scope) -> str | None:
    for name, value in scope["headers"]:
        if name.lower() == b"authorization":
            scheme, _, token = value.decode().partition(" ")
            if scheme.lower() != "bearer" or not token.strip():
                return None
            return token.strip()
    return None
