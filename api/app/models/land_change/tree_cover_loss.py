from typing import Union, List, Annotated, Literal, Optional

from models.common.analysis import AnalysisStatus
from pydantic import Field, field_validator, model_validator

from app.models.common.base import StrictBaseModel, Response
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
AllowedIntersections = List[Literal["driver", "natural_lands"]]

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
    intersections: AllowedIntersections = Field(
        ...,
        min_length=0,
        max_length=1,
        description="List of intersection types"
    )


    @field_validator('start_year', 'end_year')
    def year_must_be_at_least_2001(cls, v: str) -> str:
        year_int = int(v)
        if year_int < 2001:
            raise ValueError('Year must be at least 2001')
        return v


    @model_validator(mode='after')
    def validate_year_range(self) -> 'TreeCoverLossAnalyticsIn':
        start = int(self.start_year)
        end = int(self.end_year)
        if end < start:
            raise ValueError('end_year must be greater than or equal to start_year')
        return self


class TreeCoverLossAnalytics(StrictBaseModel):
    result: Optional[dict] = {  # column oriented for loading into a dataframe
        "__dtypes__": {
            "country": "str",
            "region": "int",
            "subregion": "int",
            "tree_cover_loss__year": "int",
            "tree_cover_loss__ha": "float",
            "gross_emissions_co2e_all_gases__mg": "float",
        },
        "country": ["BRA", "BRA", "BRA"],
        "region": [1, 1, 1],
        "subregion": [12, 12, 12],
        "tree_cover_loss__year": [2022, 2023, 2024],
        "tree_cover_loss__ha": [4045.406160862687, 4050.4061608627, 4045.406160862687],
        "gross_emissions_co2e_all_gases__mg": [3490821.6510292348, 114344.24741739516, 114347.2474174],
    }
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class TreeCoverLossAnalyticsResponse(Response):
    data: TreeCoverLossAnalytics