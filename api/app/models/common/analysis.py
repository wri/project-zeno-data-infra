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
        # Remove private attributes if in serialized data
        kwargs.pop("_version", None)
        kwargs.pop("_analytics_name", None)
        # No longer persisted, but may exist in older analyses
        kwargs.pop("_input_uris", None)

        super().__init__(**kwargs)

    _analytics_name: str = PrivateAttr(default="analytics")
    _version: str = PrivateAttr(default="v0")

    aoi: AreaOfInterest

    def model_dump(self, **kwargs):
        """Generate dictionary representation of object including private attributes"""
        result = super().model_dump(**kwargs)
        result["_version"] = self._version
        result["_analytics_name"] = self._analytics_name
        return result

    def model_dump_json(self, **kwargs):
        """Generate JSON representation of object including private attributes"""
        result = json.loads(super().model_dump_json(**kwargs))
        result["_version"] = self._version
        result["_analytics_name"] = self._analytics_name
        return json.dumps(result)

    def public_metadata(self) -> dict:
        """Return public metadata

        Bypasses the private field injection in the overridden model_dump* methods
        """
        return super().model_dump()

    def thumbprint(self) -> uuid.UUID:
        """Generate a deterministic UUID thumbprint."""
        dump_dict = self.model_dump(exclude=set(), mode="json")

        # Manually include important private attributes
        dump_dict["_version"] = self._version
        dump_dict["_analytics_name"] = self._analytics_name

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
