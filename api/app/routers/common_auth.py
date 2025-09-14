import logging
import os
from typing import Annotated

from fastapi import Header, HTTPException
from starlette import status


async def get_secret():
    secret = os.getenv("ZENO_SECRET")
    return secret


async def verify_shared_secret(
    x_client_secret: Annotated[str | None, Header(alias="X-Client-Secret")] = None,
):
    secret = await get_secret()
    if x_client_secret and x_client_secret == secret:
        logging.info(
            {"event": "shared_secret_verification_success", "user": "some friend"}
        )
    else:
        logging.warning(
            {
                "event": "shared_secret_verification_failure",
                "user": "some stranger",
                "severity": "medium",
            }
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing shared secret",
        )
