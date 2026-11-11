from typing import Annotated, Any, Dict, Literal, Union

from pydantic import Field, TypeAdapter

from .common.areas_of_interest import (
    AdminAreaOfInterest,
    IndigenousAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)
from .common.base import Response, StrictBaseModel

AoiType = Literal["admin", "key_biodiversity_area", "protected_area", "indigenous_land"]

aoi_by_type = TypeAdapter(
    Annotated[
        Union[
            AdminAreaOfInterest,
            KeyBiodiversityAreaOfInterest,
            ProtectedAreaOfInterest,
            IndigenousAreaOfInterest,
        ],
        Field(discriminator="type"),
    ]
)


class AoiFeature(StrictBaseModel):
    type: Literal["Feature"] = "Feature"
    id: str
    geometry: Dict[str, Any]
    properties: Dict[str, Any]


class AoiFeatureResponse(Response):
    data: AoiFeature
