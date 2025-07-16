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
        title="Dot-delimited identifier",
        examples=["BRA.12.3", "IND", "IDN.12"],
    )
    provider: str = Field("gadm", title="Administrative Boundary Provider")
    version: str = Field("4.1", title="Administrative Boundary Version")

    async def get_geostore_id(self) -> Optional[UUID]:
        # admin_level = self.get_admin_level()
        geostore_id = None
        return geostore_id

    def get_admin_level(self):
        admin_level = (
            sum(
                1
                for field in (self.country, self.region, self.subregion)
                if field is not None
            )
            - 1
        )
        return admin_level

    @model_validator(mode="after")
    def check_region_subregion(cls, values):
        # id = values.get("id")
        # parse id to get region and subregion (if they exist)
        subregion = None
        region = None
        if subregion is not None and region is None:
            raise ValueError("region must be specified if subregion is provided")
        return values

    @field_validator("provider", mode="before")
    def set_provider_default(cls, v):
        return v or "gadm"

    @field_validator("version", mode="before")
    def set_version_default(cls, v):
        return v or "4.1"


class KeyBiodiversityAreaOfInterest(AreaOfInterest):
    type: Literal["key_biodiversity_area"] = "key_biodiversity_area"
    ids: List[str] = Field(
        ..., title="Key Biodiversity Area site codes", examples=[["36"], ["18", "8111"]]
    )


class ProtectedAreaOfInterest(AreaOfInterest):
    type: Literal["protected_area"] = "protected_area"
    id: List[str] = Field(
        ...,
        title="WDPA protected area IDs",
        examples=[["555625448"], ["148322", "555737674"]],
    )


class IndigenousAreaOfInterest(AreaOfInterest):
    type: Literal["indigenous_land"] = "indigenous_land"
    id: List[str] = Field(
        ...,
        title="Landmark Indigenous lands object ID",
        examples=[["1931"], ["1918", "43053"]],
    )


class CustomAreaOfInterest(AreaOfInterest):
    type: Literal["feature_collection"] = "feature_collection"
    feature_collection: Dict[str, Any] = Field(
        ...,
        title="Feature collection of one or more features",
    )
