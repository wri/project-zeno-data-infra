from typing import Annotated, Optional, Union

from pydantic import Field

from ..common.analysis import AnalysisStatus
from ..common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
    IndigenousAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)
from ..common.base import Response, StrictBaseModel

AoiUnion = Union[
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
]


class GrasslandsAnalyticsIn(StrictBaseModel):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )
    start_year: Optional[int] = Field(
        2000,
        title="Start year",
        description="Must be year in YYYY format.",
        examples=["2020"],
    )
    end_year: Optional[int] = Field(
        2024,
        title="End year",
        description="Must be year in YYYY format.",
        examples=["2024"],
    )


class GrasslandsAnalytics(StrictBaseModel):
    result: Optional[dict] = {  # column oriented for loading into a dataframe
        "__dtypes__": {
            "country": "str",
            "region": "int",
            "subregion": "int",
            "year": "int",
            "grassland_area": "float",
        },
        "country": ["BRA", "BRA", "BRA"],
        "region": [1, 1, 1],
        "subregion": [12, 12, 12],
        "year": [2000, 2001, 2002],
        "grassland_area": [384.9, 483.1, 1858.3],
    }
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class GrasslandsAnalyticsResponse(Response):
    data: GrasslandsAnalytics
