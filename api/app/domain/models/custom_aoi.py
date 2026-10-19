from dataclasses import dataclass

from shapely.geometry import MultiPolygon


@dataclass(frozen=True)
class CustomAoi:
    aoi_id: str
    geometry: MultiPolygon
    area_ha: float
