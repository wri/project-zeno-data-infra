from typing import Union, List, Annotated, Literal
from pydantic import Field, field_validator

from app.models.common.base import StrictBaseModel
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
)

AoiUnion = Union[
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
]

DATE_REGEX = r"^\d{4}$"

AllowedCanopyCover = Literal[10, 15, 20, 25, 30, 50, 75]

class TreeCoverLossAnalyticsIn(StrictBaseModel):
    aois: List[Annotated[AoiUnion, Field(discriminator="type")]] = Field(
        ..., min_length=1, max_length=1, description="List of areas of interest."
    )
    start_year: str = Field(
        ...,
        title="Start Date",
        description="Must be year in YYYY date format. Minimum year is 2001.",
        pattern=DATE_REGEX,
        examples=["2020", "2024"],
    )
    end_year: str = Field(
        ...,
        title="End Date",
        description="Must be year in YYYY date format. Minimum year is 2001.",
        pattern=DATE_REGEX,
        examples=["2023", "2024"],
    )
    canopy_cover: AllowedCanopyCover = Field(
        ...,
        title="Canopy Cover",
    )
    intersections: List[Literal["driver", "natural_lands"]] = Field(
        ..., min_length=0, max_length=1, description="List of intersection types"
    )

    # Validator to ensure year is >= 2001
    @field_validator('start_year', 'end_year')
    def year_must_be_at_least_2001(cls, v: str) -> str:
        year_int = int(v)
        if year_int < 2001:
            raise ValueError('Year must be at least 2001')
        return v