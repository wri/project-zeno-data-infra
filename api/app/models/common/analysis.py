import json
import uuid
from enum import Enum
from typing import TYPE_CHECKING, List, Optional

from pydantic import PrivateAttr

from app.models.common.areas_of_interest import AreaOfInterest
from app.models.common.base import StrictBaseModel

DATE_REGEX = r"^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$"


if TYPE_CHECKING:
    from app.domain.models.environment import Environment


class AnalysisStatus(str, Enum):
    saved = "saved"
    pending = "pending"
    failed = "failed"


class AnalyticsIn(StrictBaseModel):
    def __init__(self, **kwargs):
        # Remove private attributes if in serialized data
        kwargs.pop("_version", None)
        kwargs.pop("_analytics_name", None)
        kwargs.pop("_environment", None)

        super().__init__(**kwargs)

    _analytics_name: str = PrivateAttr(default="analytics")
    _version: str = PrivateAttr(default="v0")
    _environment: str | None = PrivateAttr(default=None)

    aoi: AreaOfInterest

    def set_environment(self, environment: "Environment") -> None:
        """Fingerprint the Zarr URIs for the given environment and store it.

        Using URIs rather than the environment name means the thumbprint is
        tied to the actual data, not the label. When a staging dataset is
        promoted to production its URI stays the same, so cached results
        remain valid across that promotion. Conversely, two environments that
        happen to share all the same URIs will correctly share cached results.
        """
        from app.domain.models.dataset import Dataset
        from app.domain.repositories.zarr_dataset_repository import (
            ZarrDatasetRepository,
        )

        uris: List = sorted(
            ZarrDatasetRepository.resolve_zarr_uri(dataset, environment)
            for dataset in Dataset
        )
        self._environment = json.dumps(uris)

    def model_dump(self, **kwargs):
        """Generate dictionary representation of object including private attributes"""
        result = super().model_dump(**kwargs)
        result["_version"] = self._version
        result["_analytics_name"] = self._analytics_name
        result["_environment"] = self._environment
        return result

    def model_dump_json(self, **kwargs):
        """Generate JSON representation of object including private attributes"""
        result = json.loads(super().model_dump_json(**kwargs))
        result["_version"] = self._version
        result["_analytics_name"] = self._analytics_name
        result["_environment"] = self._environment
        return json.dumps(result)

    def thumbprint(self) -> uuid.UUID:
        """Generate a deterministic UUID thumbprint."""
        if self._environment is None:
            from app.domain.models.environment import Environment

            self.set_environment(Environment.production)

        dump_dict = self.model_dump(exclude=set(), mode="json")

        # Manually include important private attributes
        dump_dict["_version"] = self._version
        dump_dict["_analytics_name"] = self._analytics_name
        dump_dict["_environment"] = self._environment

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
