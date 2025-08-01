import json
import uuid
from typing import Union, List, Annotated, Literal, Optional
from pydantic import Field, field_validator, model_validator
from app.models.common.analysis import AnalysisStatus
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
AllowedForestFilter = Literal["primary_forest", "intact_forest"]
AllowedIntersections = List[Literal["driver"]]


class TreeCoverLossAnalyticsIn(StrictBaseModel):
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

    def thumbprint(self) -> uuid:
        """
        Generate a deterministic UUID thumbprint based on the model's JSON representation.

        Returns:
            uuid: UUID5 string derived from sorted JSON representation
        """
        payload_dict = self.model_dump()
        payload_json = json.dumps(payload_dict, sort_keys=True)
        return uuid.uuid5(uuid.NAMESPACE_DNS, payload_json)

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
    status: AnalysisStatus

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
