from typing import Annotated, List, Literal, Optional, Union

from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
    IndigenousAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)
from app.models.common.base import Response, StrictBaseModel
from pydantic import Field, PrivateAttr

AoiUnion = Union[
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
]

AllowedCanopyCover = Literal[30, 50, 75]


class CarbonFluxAnalyticsIn(AnalyticsIn):
    _version: str = PrivateAttr(default="v20250910")
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )
    canopy_cover: AllowedCanopyCover = Field(
        ...,
        title="Canopy Cover",
    )


class CarbonFluxResult(StrictBaseModel):
    aoi_id: List[str]
    aoi_type: List[str]
    carbon_net_flux_Mg_CO2e: List[float]
    carbon_gross_emissions_Mg_CO2e: List[float]
    carbon_gross_removals_Mg_CO2e: List[float]


class CarbonFluxAnalytics(StrictBaseModel):
    result: Optional[CarbonFluxResult] = None
    metadata: Optional[CarbonFluxAnalyticsIn] = None
    message: Optional[str] = None
    status: Optional[AnalysisStatus] = None

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class CarbonFluxAnalyticsResponse(Response):
    data: CarbonFluxAnalytics

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "data": {
                        "result": {  # column oriented for loading into a dataframe
                            "aoi_id": ["BRA.1.12", "BRA.1.12", "BRA.1.12"],
                            "aoi_type": ["admin", "admin", "admin"],
                            "carbon_net_flux_Mg_CO2e": [
                                4045.406160862687,
                                4050.4061608627,
                                4045.406160862687,
                            ],
                            "carbon_gross_emissions_Mg_CO2e": [
                                4045.406160862687,
                                4050.4061608627,
                                4045.406160862687,
                            ],
                            "carbon_gross_removals_Mg_CO2e": [
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
                            "canopy_cover": "30",
                        },
                        "message": "",
                        "status": "saved",
                    }
                },
            ]
        }
    }
