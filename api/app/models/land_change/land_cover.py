from typing import Annotated, List, Optional, Union

from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
    IndigenousAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)
from app.models.common.base import StrictBaseModel, Response
from pydantic import Field

AoiUnion = Union[
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
]


class LandCoverChangeAnalyticsIn(AnalyticsIn):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )


class LandCoverChangeResult(StrictBaseModel):
    aoi_id: List[str]
    aoi_type: List[str]
    land_cover_class_start: List[str]
    land_cover_class_end: List[str]
    change_area: List[float]


class LandCoverChangeAnalytics(StrictBaseModel):
    result: Optional[LandCoverChangeResult] = None
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: Optional[AnalysisStatus] = None

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class LandCoverChangeAnalyticsResponse(Response):
    data: LandCoverChangeAnalytics
