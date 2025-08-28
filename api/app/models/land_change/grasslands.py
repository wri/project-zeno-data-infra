from typing import Annotated, Optional, Union

from pydantic import Field, field_validator, model_validator

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

DATE_REGEX = r"^\d{4}$"


class GrasslandsAnalyticsIn(StrictBaseModel):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )
    start_year: str = Field(
        ...,
        title="Start Date",
        description="Must be year in YYYY date format. Minimum year is 2000.",
        pattern=DATE_REGEX,
        examples=["2020", "2024"],
    )
    end_year: str = Field(
        ...,
        title="End Date",
        description="Must be year in YYYY date format. Minimum year is 2000.",
        pattern=DATE_REGEX,
        examples=["2023", "2024"],
    )

    @field_validator("start_year", "end_year")
    def year_must_be_at_least_2001(cls, v: str) -> str:
        year_int = int(v)
        if year_int < 2000:
            raise ValueError("Year must be at least 2000")
        return v

    @model_validator(mode="after")
    def validate_year_range(self) -> "GrasslandsAnalyticsIn":
        start = int(self.start_year)
        end = int(self.end_year)
        if end < start:
            raise ValueError("end_year must be greater than or equal to start_year")
        return self


class GrasslandsAnalytics(StrictBaseModel):
    result: Optional[dict] = None
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class GrasslandsAnalyticsResponse(Response):
    data: GrasslandsAnalytics
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "data": {
                        "result": {  # column oriented for loading into a dataframe
                            "aoi_id": ["BRA.1.12", "BRA.1.12", "BRA.1.12"],
                            "aoi_type": ["admin", "admin", "admin"],
                            "year": [2000, 2001, 2002],
                            "area_ha": [384.9, 483.1, 1858.3],
                        },
                        "metadata": {
                            "aoi": {
                                "type": "admin",
                                "ids": ["BRA.1.12"],
                            },
                        },
                        "message": "",
                        "status": "saved",
                    }
                }
            ]
        }
    }
