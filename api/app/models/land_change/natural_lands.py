from typing import Annotated, Optional, Union

from pydantic import Field

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


class NaturalLandsAnalyticsIn(StrictBaseModel):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )


class NaturalLandsAnalytics(StrictBaseModel):
    result: Optional[dict] = None
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class NaturalLandsAnalyticsResponse(Response):
    data: NaturalLandsAnalytics
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
                                "natural_lands_area": "float32",
                            },
                            "country": ["BRA", "BRA", "BRA"],
                            "region": [1, 1, 1],
                            "subregion": [12, 12, 12],
                            "natural_lands_area": [4851452000, 4084129000, 253008000],
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
