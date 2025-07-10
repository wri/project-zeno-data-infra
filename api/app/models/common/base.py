from typing import Any

from pydantic import BaseModel


class StrictBaseModel(BaseModel):
    model_config = {
        "extra": "forbid",
        "validate_assignment": True,
    }


class Response(StrictBaseModel):
    data: Any
    status: str = "success"


class DataMartResourceLink(StrictBaseModel):
    link: str


class DataMartResourceLinkResponse(Response):
    data: DataMartResourceLink
