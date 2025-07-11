from typing import Optional, List, Annotated, Union, Literal
from pydantic import Field

from ..common.analysis import AnalysisStatus, DATE_REGEX
from ..common.areas_of_interest import (
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
)
from ..common.base import StrictBaseModel, Response


AoiUnion = Union[
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
]


class DistAlertsAnalyticsIn(StrictBaseModel):
    aois: List[Annotated[AoiUnion, Field(discriminator="type")]] = Field(
        ..., min_length=1, max_length=1, description="List of areas of interest."
    )
    start_date: str = Field(
        ...,
        title="Start Date",
        description="Must be either year or YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2020", "2020-01-01"],
    )
    end_date: str = Field(
        ...,
        title="End Date",
        description="Must be either year or YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2023", "2023-12-31"],
    )
    intersections: List[Literal["driver", "natural_lands"]] = Field(
        ..., min_length=0, max_length=1, description="List of intersection types"
    )


class DistAlertsAnalytics(StrictBaseModel):
    result: Optional[dict] = {  # column oriented for loading into a dataframe
        "__dtypes__": {
            "country": "str",
            "region": "int",
            "subregion": "int",
            "alert_date": "int",
            "confidence": "int",
            "value": "int",
        },
        "country": ["BRA", "BRA", "BRA"],
        "region": [1, 1, 1],
        "subregion": [12, 12, 12],
        "alert_date": [731, 733, 733],
        "confidence": [2, 2, 3],
        "value": [38, 5, 3],
    }
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class DistAlertsAnalyticsResponse(Response):
    data: DistAlertsAnalytics
