import json
import uuid
from typing import Annotated, Literal, Optional, Union

from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
)
from app.models.common.base import Response, StrictBaseModel
from pydantic import Field

AoiUnion = Union[
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
]

AllowedCanopyCover = Literal[10, 15, 20, 25, 30, 50, 75]
AllowedForestFilter = Literal["primary_forest", "intact_forest"]


class TreeCoverAnalyticsIn(StrictBaseModel):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )
    canopy_cover: AllowedCanopyCover = Field(
        ...,
        title="Canopy Cover",
    )
    forest_filter: AllowedForestFilter | None = Field(
        default=None,
        title="Forest Filter",
    )

    def thumbprint(self) -> uuid.UUID:
        payload_dict = self.model_dump()
        payload_json = json.dumps(payload_dict, sort_keys=True)
        return uuid.uuid5(uuid.NAMESPACE_DNS, payload_json)


class TreeCoverAnalytics(StrictBaseModel):
    result: Optional[dict] = None
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class TreeCoverAnalyticsResponse(Response):
    data: TreeCoverAnalytics

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "data": {
                        "result": {
                            "__dtypes__": {
                                "country": "str",
                                "region": "int",
                                "subregion": "int",
                                "tree_cover__year": "int",
                                "tree_cover__ha": "float",
                            },
                            "country": ["BRA", "BRA", "BRA"],
                            "region": [1, 1, 1],
                            "subregion": [12, 12, 12],
                            "tree_cover__year": [2022, 2023, 2024],
                            "tree_cover__ha": [
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
