from typing import Any, Dict

from pydantic import BaseModel


class User(BaseModel):
    """A ResourceWatch user as returned by the RW API's check-logged endpoint.

    Only the fields we authorize on are declared; RW returns more (email,
    provider, createdAt, ...) which are ignored.
    """

    id: str
    role: str
    extraUserData: Dict[str, Any] = {}

    model_config = {"extra": "ignore"}
