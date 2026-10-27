from typing import Annotated, Any, Dict, List, Literal, Union

from pydantic import Field, TypeAdapter

from .common.areas_of_interest import (
    AdminAreaOfInterest,
    IndigenousAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)
from .common.base import Response, StrictBaseModel

PredefinedAoiType = Literal[
    "admin", "key_biodiversity_area", "protected_area", "indigenous_land"
]

predefined_aoi = TypeAdapter(
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


class AoiMatchOut(StrictBaseModel):
    aoi_type: PredefinedAoiType
    aoi_id: str
    name: str
    display_name: str


class AoiSearchResponse(Response):
    data: List[AoiMatchOut]


class AoiFeature(StrictBaseModel):
    type: Literal["Feature"] = "Feature"
    id: str
    geometry: Dict[str, Any]
    properties: Dict[str, Any]


class AoiFeatureResponse(Response):
    data: AoiFeature
