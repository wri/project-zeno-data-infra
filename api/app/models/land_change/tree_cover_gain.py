from typing import Annotated, Optional, Union

from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
)
from app.models.common.base import Response, StrictBaseModel
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


class TreeCoverGainAnalytics(StrictBaseModel):
    result: Optional[dict] = None
    metadata: Optional[dict] = None
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
                            "tree_cover_loss__ha": [
                                4045.406160862687,
                                4050.4061608627,
                                4045.406160862687,
                            ],
                            "gross_emissions_co2e_all_gases__mg": [
                                3490821.6510292348,
                                114344.24741739516,
                                114347.2474174,
                            ],
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
