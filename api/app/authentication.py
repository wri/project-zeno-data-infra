import os

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.infrastructure.external_services.resource_watch_authenticator import (
    ResourceWatchAuthenticator,
    UpstreamAuthError,
)
from app.models.common.authentication import User

bearer_scheme = HTTPBearer(auto_error=False)


def get_authenticator() -> ResourceWatchAuthenticator:
    """Edge-injected authenticator. Reads the RW API base from the environment so
    it can be pointed at staging without code changes; defaults to production."""
    return ResourceWatchAuthenticator(
        api_url=os.getenv("RW_API_URL", "https://api.resourcewatch.org")
    )


async def require_resource_watch_admin(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    authenticator: ResourceWatchAuthenticator = Depends(get_authenticator),
) -> User:
    """Guard that only lets ResourceWatch admins through.

    401 when the token is missing or invalid; 403 when the token is valid but the
    user is not an admin.
    """
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        user = await authenticator.get_user(credentials.credentials)
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
