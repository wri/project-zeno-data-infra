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
from pydantic import Field, field_validator, model_validator

AoiUnion = Union[
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
]

DATE_REGEX = r"^\d{4}$"


class TreeCoverGainAnalyticsIn(AnalyticsIn):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )
    start_year: str = Field(
        ...,
        title="Start Date",
        description="Must be year in YYYY date format. Minimum year is 2000. Must be a multiple of 5 (e.g., 2000, 2005).",
        pattern=DATE_REGEX,
        examples=["2000", "2005"],
    )
    end_year: str = Field(
        ...,
        title="End Date",
        description="Must be year in YYYY date format. Minimum year is 2000. Must be a multiple of 5 (e.g., 2000, 2005).",
        pattern=DATE_REGEX,
        examples=["2000", "2005"],
    )

    @field_validator("start_year", "end_year")
    def year_must_be_at_least_2001(cls, v: str) -> str:
        year_int = int(v)
        if year_int < 2000:
            raise ValueError("Year must be at least 2001")

        if year_int % 5 != 0:
            raise ValueError("Year must be a multiple of 5 (e.g., 2000, 2005, 2010)")

        return v

    @model_validator(mode="after")
    def validate_year_range(self) -> "TreeCoverGainAnalyticsIn":
        start = int(self.start_year)
        end = int(self.end_year)
        if end < start:
            raise ValueError("end_year must be greater than or equal to start_year")
        return self


class TreeCoverGainResult(StrictBaseModel):
    id: List[str]
    tree_cover_gain__year: List[int]
    tree_cover_gain_area__ha: List[float]


class TreeCoverGainAnalytics(StrictBaseModel):
    result: Optional[TreeCoverGainResult] = None
    metadata: Optional[TreeCoverGainAnalyticsIn] = None
    message: Optional[str] = None
    status: Optional[AnalysisStatus] = None

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class TreeCoverGainAnalyticsResponse(Response):
    data: TreeCoverGainAnalytics

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "data": {
                        "result": {  # column oriented for loading into a dataframe
                            "id": ["BRA.1.12", "BRA.1.12", "BRA.1.12"],
                            "tree_cover_gain__year": [2000, 2005, 2010],
                            "tree_cover_gain_area__ha": [
                                4045.406160862687,
                                4050.4061608627,
                                4045.406160862687,
                            ],
                        },
                        "metadata": {
                            "aoi": {
                                "type": "admin",
                                "ids": ["BRA.1.12"],
                            },
                            "start_year": "2000",
                            "end_year": "2010",
                        },
                        "message": "",
                        "status": "saved",
                    }
                },
            ]
        }
    }
