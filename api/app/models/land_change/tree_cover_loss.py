from typing import Annotated, List, Literal, Optional, Union

from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
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
]

DATE_REGEX = r"^\d{4}$"
AllowedCanopyCover = Literal[10, 15, 20, 25, 30, 50, 75]
AllowedForestFilter = Literal["primary_forest", "intact_forest"]
AllowedIntersections = List[Literal["driver"]]


class TreeCoverLossAnalyticsIn(AnalyticsIn):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
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
    forest_filter: AllowedForestFilter | None = Field(
        default=None,
        title="Forest Filter",
    )
    intersections: AllowedIntersections = Field(
        ..., min_length=0, max_length=1, description="List of intersection types"
    )

    @field_validator("start_year", "end_year")
    def year_must_be_at_least_2001(cls, v: str) -> str:
        year_int = int(v)
        if year_int < 2001:
            raise ValueError("Year must be at least 2001")
        return v

    @model_validator(mode="after")
    def validate_year_range(self) -> "TreeCoverLossAnalyticsIn":
        start = int(self.start_year)
        end = int(self.end_year)
        if end < start:
            raise ValueError("end_year must be greater than or equal to start_year")
        return self


class TreeCoverLossAnalytics(StrictBaseModel):
    result: Optional[dict] = None
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: Optional[AnalysisStatus] = None

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class TreeCoverLossAnalyticsResponse(Response):
    data: TreeCoverLossAnalytics

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "data": {
                        "result": {  # column oriented for loading into a dataframe
                            "__dtypes__": {
                                "aoi_id": "str",
                                "aoi_type": "str",
                                "tree_cover_loss_year": "int",
                                "area__ha": "float",
                            },
                            "aoi_id": ["IDN.24.9", "IDN.14.4", "BRA.1.1"],
                            "aoi_type": ["admin", "admin", "admin"],
                            "tree_cover_loss_year": [2022, 2023, 2024],
                            "area__ha": [
                                4045.406160862687,
                                4050.4061608627,
                                4045.406160862687,
                            ],
                            # "gross_emissions_co2e_all_gases__mg": [
                            #     3490821.6510292348,
                            #     114344.24741739516,
                            #     114347.2474174,
                            # ],
                        },
                        "metadata": {
                            "aoi": {
                                "type": "admin",
                                "ids": ["BRA.1.12"],
                            },
                            "start_year": "2022",
                            "end_year": "2024",
                            "canopy_cover": "30",
                        },
                        "message": "",
                        "status": "saved",
                    }
                },
            ]
        }
    }
