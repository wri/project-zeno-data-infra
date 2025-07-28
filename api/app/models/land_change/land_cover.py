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
