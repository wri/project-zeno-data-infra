from typing import Annotated, List, Literal, Optional, Union

from pydantic import Field

from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
    IndigenousAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)
from app.models.common.base import Response, StrictBaseModel
from app.models.land_change.tree_cover_loss import AllowedForestFilter

AoiUnion = Union[
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
]


ValidCanopyCover = Literal[10, 15, 20, 25, 30, 50, 75]


class TreeCoverAnalyticsIn(AnalyticsIn):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )
    canopy_cover: ValidCanopyCover = Field(
        ...,
        title="Canopy cover threshold",
        description="Minimum canopy density to consider tree cover, in percent.",
        examples=[10, 35],
    )
    forest_filter: AllowedForestFilter | None = Field(
        default=None,
        title="Forest Filter",
    )


class TreeCoverResult(StrictBaseModel):
    aoi_id: List[str]
    aoi_type: List[str]
    area_ha: List[float]


class TreeCoverAnalytics(StrictBaseModel):
    result: Optional[TreeCoverResult] = None
    metadata: Optional[TreeCoverAnalyticsIn] = None
    message: Optional[str] = None
    status: Optional[AnalysisStatus] = None

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class TreeCoverAnalyticsResponse(Response):
    data: TreeCoverAnalytics

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "data": {
                        "result": {  # column oriented for loading into a dataframe
                            "aoi_id": ["BRA.1.12"],
                            "aoi_type": ["admin"],
                            "area_ha": [
                                4025.406160862687,
                            ],
                        },
                        "metadata": {
                            "aoi": {
                                "type": "admin",
                                "ids": ["BRA.1.12"],
                            },
                            "canopy_cover": 10,
                            "forest_filter": "primary_forest",
                        },
                        "message": "",
                        "status": "saved",
                    }
                },
            ]
        }
    }
