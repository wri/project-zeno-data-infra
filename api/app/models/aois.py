from typing import Annotated, Any, Dict, List, Literal, Union

from pydantic import Field, TypeAdapter

from .common.areas_of_interest import (
    AdminAreaOfInterest,
    IndigenousAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)
from .common.base import Response, StrictBaseModel

AoiType = Literal["admin", "key_biodiversity_area", "protected_area", "indigenous_land"]
SearchableAoiType = Literal[
    "admin", "key_biodiversity_area", "protected_area", "indigenous_land", "custom_aoi"
]

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


class AoiMatchOut(StrictBaseModel):
    aoi_type: SearchableAoiType
    aoi_id: str
    name: str
    display_name: str


class AoiSearchResponse(Response):
    data: List[AoiMatchOut]


class AoiSubregionOut(StrictBaseModel):
    aoi_type: AoiType
    aoi_id: str
    name: str


class AoiSubregionsResponse(Response):
    total: int = Field(..., description="How many subregions there are in all.")
    data: List[AoiSubregionOut]


class AoiFeature(StrictBaseModel):
    type: Literal["Feature"] = "Feature"
    id: str
    geometry: Dict[str, Any]
    properties: Dict[str, Any]


class AoiFeatureResponse(Response):
    data: AoiFeature
