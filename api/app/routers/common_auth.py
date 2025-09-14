import logging
import os
from typing import Annotated

from fastapi import Header


async def get_secret():
    secret = os.getenv("ZENO_SECRET")
    return secret


async def verify_shared_secret(
    x_client_secret: Annotated[str | None, Header(alias="X-Client-Secret")] = None,
):
    secret = await get_secret()
    if not x_client_secret or x_client_secret != secret:
        logging.warning(
            {"event": "shared_secret_verification_failure", "user": "some stranger"}
        )
        # Once the front-end is ready, short-circuit the request with 401
        # raise HTTPException(
        #     status_code=status.HTTP_401_UNAUTHORIZED,
        #     detail="Invalid or missing shared secret",
        # )
    else:
        logging.info(
            {"event": "shared_secret_verification_success", "user": "some friend"}
        )
