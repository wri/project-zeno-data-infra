from typing import Optional

from pydantic import Field, PrivateAttr

from ..common.analysis import AnalysisStatus, AnalyticsIn
from ..common.areas_of_interest import AdminAreaOfInterest
from ..common.base import Response, StrictBaseModel

ANALYTICS_NAME = "land_ghg_inventory"


class LandGhgInventoryAnalyticsIn(AnalyticsIn):
    _analytics_name: str = PrivateAttr(default=ANALYTICS_NAME)
    _version: str = PrivateAttr(default="v20260723")
    aoi: AdminAreaOfInterest = Field(
        ...,
        title="AOI",
        description="GADM admin area to summarize. GADM areas only (no on-the-fly).",
    )


class LandGhgInventoryAnalytics(StrictBaseModel):
    result: Optional[dict] = None
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class LandGhgInventoryAnalyticsResponse(Response):
    data: LandGhgInventoryAnalytics
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "data": {
                        "result": {  # column oriented for loading into a dataframe
                            "aoi_id": ["BRA.1", "BRA.1"],
                            "aoi_type": ["admin", "admin"],
                            "land_state_class": ["tree_loss", "tree_gain"],
                            "year": [2016, 2016],
                            "gross_emissions_MgCO2e": [438480933.0, 0.0],
                            "gross_removals_MgCO2": [-29404371.0, -3213337.0],
                            "net_flux_MgCO2e": [409076565.0, -3213337.0],
                            "area_ha": [1086087.0, 518423.0],
                        },
                        "metadata": {
                            "aoi": {
                                "type": "admin",
                                "ids": ["BRA.1"],
                            },
                        },
                        "message": "",
                        "status": "saved",
                    }
                }
            ]
        }
    }
