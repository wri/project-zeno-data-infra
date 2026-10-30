from typing import Any, ClassVar, Dict, List

import shapely
from pydantic import BaseModel, Field, field_validator, model_validator
from shapely.geometry import shape


class CustomAoiIn(BaseModel):
    id: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    geometry: Dict[str, Any] = Field(
        ..., description="GeoJSON Polygon or MultiPolygon, in WGS84."
    )
    # keeps repair and area calculation within one request's time budget
    max_coordinates: ClassVar[int] = 100_000

    @field_validator("geometry")
    @classmethod
    def must_be_polygonal_geojson(cls, geometry):
        if geometry.get("type") not in ("Polygon", "MultiPolygon"):
            raise ValueError("geometry must be a GeoJSON Polygon or MultiPolygon")
        coordinates = shapely.get_num_coordinates(shape(geometry))
        if coordinates > cls.max_coordinates:
            raise ValueError(
                f"geometry has {coordinates} coordinates; "
                f"the limit is {cls.max_coordinates}"
            )
        return geometry


class CustomAoisIn(BaseModel):
    aois: List[CustomAoiIn] = Field(..., min_length=1, max_length=500)

    @model_validator(mode="after")
    def ids_must_be_unique(self):
        ids = [aoi.id for aoi in self.aois]
        if len(ids) != len(set(ids)):
            raise ValueError("AOI ids must be unique")
        return self
