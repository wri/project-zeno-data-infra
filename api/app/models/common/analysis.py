import json
import uuid
from enum import Enum
from typing import Optional

from pydantic import PrivateAttr

from app.models.common.areas_of_interest import AreaOfInterest
from app.models.common.base import StrictBaseModel

DATE_REGEX = r"^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$"


class AnalysisStatus(str, Enum):
    saved = "saved"
    pending = "pending"
    failed = "failed"


class AnalyticsIn(StrictBaseModel):
    def __init__(self, **kwargs):
        kwargs.pop("_version", None)  # remove _version if in serialized data
        kwargs.pop(
            "_analytics_name", None
        )  # remove _analytics_name if in serialized data
        super().__init__(**kwargs)

    _analytics_name: str = PrivateAttr(default="analytics")
    _version: str = PrivateAttr(default="v0")

    aoi: AreaOfInterest

    def model_dump(self, **kwargs):
        """Add the _version private attribute"""
        result = super().model_dump(**kwargs)
        result["_version"] = self._version
        result["_analytics_name"] = self._analytics_name
        return result

    def model_dump_json(self, **kwargs):
        """Add the _version private attribute"""
        result = json.loads(super().model_dump_json(**kwargs))
        result["_version"] = self._version
        result["_analytics_name"] = self._analytics_name
        return json.dumps(result)

    def thumbprint(self) -> uuid.UUID:
        """
        Generate a deterministic UUID thumbprint including the version.
        """
        # Include version and analytics_name in dump for thumbprint consistency
        dump_dict = self.model_dump(exclude=set())
        dump_dict["_version"] = self._version  # Manually include version
        dump_dict["_analytics_name"] = (
            self._analytics_name
        )  # Manually include analytics_name
        payload_json = json.dumps(dump_dict, sort_keys=True)
        return uuid.uuid5(uuid.NAMESPACE_DNS, payload_json)


class AnalyticsOut(StrictBaseModel):
    result: Optional[dict] = None
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: Optional[AnalysisStatus] = None

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }
