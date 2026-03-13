from fastapi import Header

from app.domain.models.environment import Environment


async def get_environment(
    x_environment: Environment | None = Header(default=None),
) -> Environment:
    """Resolve the target environment from the x-environment request header.

    Defaults to production when the header is absent, so existing clients
    require no changes. Non-production environments will later require a
    bearer token; that gate is not enforced here yet.
    """
    return x_environment or Environment.production
