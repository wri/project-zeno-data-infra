import json
import uuid
from typing import Annotated, Optional, Union

from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    IndigenousAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)
from app.models.common.base import StrictBaseModel
from pydantic import Field

AoiUnion = Union[
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    IndigenousAreaOfInterest,
]


class LandCoverChangeAnalyticsIn(StrictBaseModel):
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )

    def thumbprint(self) -> uuid.UUID:
        """
        Generate a deterministic UUID thumbprint based on the model's JSON representation.

        Returns:
            uuid: UUID5 string derived from sorted JSON representation
        """
        # Convert model to dictionary with default settings
        payload_dict = self.model_dump(include=["aoi"])

        payload_json = json.dumps(payload_dict, sort_keys=True)
        return uuid.uuid5(uuid.NAMESPACE_DNS, payload_json)


class LandCoverChangeAnalytics(StrictBaseModel):
    result: Optional[dict] = None
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class LandCoverChangeAnalyticsResponse(StrictBaseModel):
    data: LandCoverChangeAnalytics

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
