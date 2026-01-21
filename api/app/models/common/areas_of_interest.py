from typing import Annotated, Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import Field, StringConstraints, field_validator, model_validator

from .base import StrictBaseModel

ADMIN_REGEX = r"^[A-Z]{3}(\.\d+)*$"
AdminStr = Annotated[str, StringConstraints(pattern=ADMIN_REGEX)]


class AreaOfInterest(StrictBaseModel):
    async def get_geostore_id(self) -> Optional[UUID]:
        """Return the unique identifier for the area of interest."""
        raise NotImplementedError("This method is not implemented.")


class AdminAreaOfInterest(AreaOfInterest):
    type: Literal["admin"] = "admin"
    ids: List[AdminStr] = Field(
        ...,
        min_length=1,
        title="List of Dot-delimited identifiers",
        examples=[["BRA.12.3"], ["BRA.12.3", "IND", "IDN.12"]],
    )
    provider: str = Field("gadm", title="Administrative Boundary Provider")
    version: str = Field("4.1", title="Administrative Boundary Version")

    async def get_geostore_id(self) -> Optional[UUID]:
        # admin_level = self.get_admin_level()
        geostore_id = None
        return geostore_id

    def get_ids_at_admin_level(self, admin_level):
        ids = []
        for id_str in self.ids:
            parts = id_str.split(".")
            if len(parts) == 1 and admin_level == 0:
                ids.append(parts[0])
            elif len(parts) == 2 and admin_level == 1:
                ids.append((parts[0], int(parts[1])))
            elif len(parts) == 3 and admin_level == 2:
                ids.append((parts[0], int(parts[1]), int(parts[2])))

        return ids

    @field_validator("ids", mode="after")
    def check_max_admin_level_2(cls, v):
        for i in v:
            if len(i.split(".")) > 3:
                raise ValueError("Maximum admin level allowed is 2")
        return v

    @field_validator("provider", mode="before")
    def set_provider_default(cls, v):
        return v or "gadm"

    @field_validator("version", mode="before")
    def set_version_default(cls, v):
        return v or "4.1"


class KeyBiodiversityAreaOfInterest(AreaOfInterest):
    type: Literal["key_biodiversity_area"] = "key_biodiversity_area"
    ids: List[str] = Field(
        ...,
        min_length=1,
        title="List of Key Biodiversity Area site codes",
        examples=[["36"], ["18", "8111"]],
    )


class ProtectedAreaOfInterest(AreaOfInterest):
    type: Literal["protected_area"] = "protected_area"
    ids: List[str] = Field(
        ...,
        min_length=1,
        title="List of WDPA protected area IDs",
        examples=[["555625448"], ["148322", "555737674"]],
    )


class IndigenousAreaOfInterest(AreaOfInterest):
    type: Literal["indigenous_land"] = "indigenous_land"
    ids: List[str] = Field(
        ...,
        min_length=1,
        title="List of Landmark Indigenous lands object ID",
        examples=[["MEX11287"], ["CAN1", "CAN2"]],
    )


class CustomAreaOfInterest(AreaOfInterest):
    type: Literal["feature_collection"] = "feature_collection"
    feature_collection: Dict[str, Any] = Field(
        ...,
        title="Feature collection of one or more features",
    )
    ids: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _extract_ids(self):
        fc = self.feature_collection

        if fc.get("type") != "FeatureCollection":
            raise ValueError("feature_collection.type must be 'FeatureCollection'")

        features = fc.get("features")
        if not isinstance(features, list) or not features:
            raise ValueError("feature_collection.features must be a non-empty list")

        ids: List[str] = []
        missing_at: List[int] = []

        for i, feat in enumerate(features):
            if not isinstance(feat, dict):
                raise ValueError(f"feature_collection.features[{i}] must be an object")

            fid = feat.get("id")
            if fid is None or (isinstance(fid, str) and not fid.strip()):
                missing_at.append(i)
            else:
                ids.append(str(fid))  # coerce int -> str if needed

        if missing_at:
            raise ValueError(
                "Each feature must have a non-empty 'id'. Missing/empty at indices: "
                + ", ".join(map(str, missing_at))
            )

        if len(set(ids)) != len(ids):
            raise ValueError("Feature ids must be unique")

        # Set "ids" this way to avoid re-triggering model_validator after hook again
        object.__setattr__(self, "ids", ids)

        return self
