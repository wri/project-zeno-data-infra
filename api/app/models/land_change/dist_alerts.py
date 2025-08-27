from typing import Annotated, List, Literal, Optional, Union

from pydantic import Field

from ..common.analysis import DATE_REGEX, AnalysisStatus
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


class DistAlertsAnalyticsIn(StrictBaseModel):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
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
    intersections: List[
        Literal["driver", "natural_lands", "grasslands", "land_cover"]
    ] = Field(..., min_length=0, max_length=1, description="List of intersection types")


class DistAlertsAnalytics(StrictBaseModel):
    result: Optional[dict] = {  # column oriented for loading into a dataframe
        "country": ["BRA", "BRA", "BRA"],
        "region": [1, 1, 1],
        "subregion": [12, 12, 12],
        "aoi_id": ["BRA.12.1", "BRA.12.1", "BRA.12.1"],
        "aoi_type": ["admin", "admin", "admin"],
        "dist_alert_date": [731, 733, 733],
        "dist_alert_confidence": [2, 2, 3],
        "area_ha": [38, 5, 3],
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
