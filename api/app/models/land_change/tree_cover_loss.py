from typing import Union, List, Annotated, Literal
from pydantic import Field

from app.models.common.base import StrictBaseModel

from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
)


AoiUnion = Union[
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
]

DATE_REGEX = r"^\d{4}$"


class TreeCoverLossAnalyticsIn(StrictBaseModel):
    aois: List[Annotated[AoiUnion, Field(discriminator="type")]] = Field(
        ..., min_length=1, max_length=1, description="List of areas of interest."
    )
    start_year: str = Field(
        ...,
        title="Start Date",
        description="Must be either year or YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2020", "2024"],
    )
    end_year: str = Field(
        ...,
        title="End Date",
        description="Must be either year or YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2023", "2024"],
    )
    canopy_cover: int = Field(
        ...,
        title="Canopy Cover",
    )
    intersections: List[Literal["driver", "natural_lands"]] = Field(
        ..., min_length=0, max_length=1, description="List of intersection types"
    )
