from typing import Any, Dict, List

from pydantic import BaseModel, Field, field_validator, model_validator
from shapely.geometry import shape


class CustomAoiIn(BaseModel):
    id: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    geometry: Dict[str, Any] = Field(
        ..., description="GeoJSON Polygon or MultiPolygon, in WGS84."
    )

    @field_validator("geometry")
    @classmethod
    def must_be_polygonal_geojson(cls, geometry):
        """A GeoJSON Polygon or MultiPolygon with at least one polygon. Empty
        polygons in a MultiPolygon hold no area, so they are dropped."""
        if geometry.get("type") not in ("Polygon", "MultiPolygon"):
            raise ValueError("geometry must be a GeoJSON Polygon or MultiPolygon")
        if geometry["type"] == "MultiPolygon":
            polygons = geometry.get("coordinates") or []
            geometry = {**geometry, "coordinates": [p for p in polygons if p]}
        try:
            parsed = shape(geometry)
        except Exception as error:
            raise ValueError(f"geometry is not valid GeoJSON: {error!r}") from error
        if parsed.is_empty:
            raise ValueError("geometry has no polygons")
        return geometry


class CustomAoisIn(BaseModel):
    aois: List[CustomAoiIn] = Field(..., min_length=1, max_length=500)

    @model_validator(mode="after")
    def ids_must_be_unique(self):
        ids = [aoi.id for aoi in self.aois]
        if len(ids) != len(set(ids)):
            raise ValueError("AOI ids must be unique")
        return self
