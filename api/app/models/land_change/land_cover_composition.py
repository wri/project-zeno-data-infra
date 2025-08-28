from typing import Annotated, List, Optional, Union

from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
    IndigenousAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)
from app.models.common.base import Response, StrictBaseModel
from pydantic import Field

AoiUnion = Union[
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
]


class LandCoverCompositionAnalyticsIn(AnalyticsIn):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )


class LandCoverCompositionResult(StrictBaseModel):
    aoi_id: List[str]
    aoi_type: List[str]
    land_cover_class: List[str]
    area_ha: List[float]


class LandCoverCompositionAnalytics(StrictBaseModel):
    result: Optional[LandCoverCompositionResult] = None
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: Optional[AnalysisStatus] = None

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class LandCoverCompositionAnalyticsResponse(Response):
    data: LandCoverCompositionAnalytics
