from typing import Annotated, Union

from app.models.common.analysis import AnalyticsIn
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
)
from pydantic import Field, field_validator, model_validator

AoiUnion = Union[
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
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
