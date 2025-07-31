import json
import uuid
from enum import Enum

from api.app.models.common.base import StrictBaseModel

DATE_REGEX = r"^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$"


class AnalysisStatus(str, Enum):
    saved = "saved"
    pending = "pending"
    failed = "failed"


class AnalyticsIn(StrictBaseModel):
    def thumbprint(self) -> uuid.UUID:
        """
        Generate a deterministic UUID thumbprint based on the model's JSON representation.

        Returns:
            uuid: UUID5 string derived from sorted JSON representation
        """
        # Convert model to dictionary with default settings
        payload_dict = self.model_dump()

        payload_json = json.dumps(payload_dict, sort_keys=True)
        return uuid.uuid5(uuid.NAMESPACE_DNS, payload_json)
