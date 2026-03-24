import logging

from fastapi import Header

from app.domain.models.environment import Environment


async def get_environment(
    x_environment: Environment | None = Header(default=None),
) -> Environment:
    """Resolve the data environment from the x-environment request header.

    Defaults to production when the header is absent, so existing clients
    require no changes. Non-production environments will later require a
    bearer token; that gate is not enforced here yet.
    """
    resolved_environment = (
        x_environment if x_environment is not None else Environment.production
    )
    logging.info(
        {
            "event": "get_environment_called",
            "specified_environment": repr(x_environment),
            "resolved_environment": resolved_environment,
        }
    )

    return resolved_environment
