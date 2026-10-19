from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from shapely.geometry import shape


class CustomAreaFeature(BaseModel):
    type: Literal["Feature"]
    id: str = Field(..., min_length=1)
    geometry: Dict[str, Any]
    properties: Optional[Dict[str, Any]] = None

    @field_validator("geometry")
    @classmethod
    def must_be_polygonal_geojson(cls, geometry):
        if geometry.get("type") not in ("Polygon", "MultiPolygon"):
            raise ValueError("geometry must be a GeoJSON Polygon or MultiPolygon")
        shape(geometry)
        return geometry


class CustomAreasIn(BaseModel):
    type: Literal["FeatureCollection"]
    features: List[CustomAreaFeature] = Field(..., min_length=1, max_length=500)

    @model_validator(mode="after")
    def ids_must_be_unique(self):
        ids = [feature.id for feature in self.features]
        if len(ids) != len(set(ids)):
            raise ValueError("feature ids must be unique")
        return self
