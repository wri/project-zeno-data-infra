from typing import Annotated, Optional, Union

from pydantic import Field, PrivateAttr

from ..common.analysis import AnalysisStatus, AnalyticsIn
from ..common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
    IndigenousAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)
from ..common.base import Response, StrictBaseModel

ANALYTICS_NAME = "integrated_alerts"

# Full YYYY-MM-DD dates only; year-only values are not accepted.
DATE_REGEX = r"^\d{4}-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])$"

AoiUnion = Union[
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
]


class IntegratedAlertsAnalyticsIn(AnalyticsIn):
    _analytics_name: str = PrivateAttr(default=ANALYTICS_NAME)
    _version: str = PrivateAttr(default="v20260601")
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )
    start_date: str = Field(
        ...,
        title="Start Date",
        description="Must be in YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2023-01-01"],
    )
    end_date: str = Field(
        ...,
        title="End Date",
        description="Must be in YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2024-12-31"],
    )


class IntegratedAlertsAnalytics(StrictBaseModel):
    result: Optional[dict] = {  # column oriented for loading into a dataframe
        "aoi_id": ["BRA.1.12", "BRA.1.12", "BRA.1.12", "BRA.1.12"],
        "aoi_type": ["admin", "admin", "admin", "admin"],
        "alert_date": ["2024-01-01", "2024-01-03", "2024-01-03", "2024-01-03"],
        "alert_confidence": ["low", "low", "high", "highest"],
        "area_ha": [38, 5, 3, 2],
    }
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class IntegratedAlertsAnalyticsResponse(Response):
    data: IntegratedAlertsAnalytics
