from typing import Annotated, List, Optional, Union, Literal

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

AoiUnion = Union[
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
]


ValidTCD = Literal["10", "15", "20", "25", "30", "50", "75", "90"]


class TreeCoverAnalyticsIn(AnalyticsIn):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )
    tcd_threshold: ValidTCD = Field(
        ...,
        title="Tree cover density threshold",
        description="Minimum canopy density to consider tree cover, in percent.",
        examples=["10", "35"],
    )

    # @field_validator("tcd_threshold")
    # def year_must_be_at_least_2000(cls, v: str) -> str:
    #     year_int = int(v)
    #     if year_int < 2000:
    #         raise ValueError("Year must be at least 2000")
    #
    #     return v
    #
    # @model_validator(mode="after")
    # def validate_year_range(self) -> "TreeCoverAnalyticsIn":
    #     start = int(self.start_year)
    #     end = int(self.end_year)
    #     if end < start:
    #         raise ValueError("end_year must be greater than or equal to start_year")
    #     return self


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
                            "aoi_id": ["BRA.1.12", "BRA.1.12", "BRA.1.12"],
                            "aoi_type": ["admin", "admin", "admin"],
                            "area_ha": [
                                4025.406160862687,
                            ],
                        },
                        "metadata": {
                            "aoi": {
                                "type": "admin",
                                "ids": ["BRA.1.12"],
                            },
                            "tcd_threshold": "10",
                        },
                        "message": "",
                        "status": "saved",
                    }
                },
            ]
        }
    }
