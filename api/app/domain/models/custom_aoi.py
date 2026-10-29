from dataclasses import dataclass

from shapely.geometry import MultiPolygon


@dataclass(frozen=True)
class CustomAoi:
    aoi_id: str
    geometry: MultiPolygon
    area_ha: float
    name: str
    user_id: str


@dataclass(frozen=True)
class CustomAoiGeometry:
    """A stored custom AOI's geometry, without its owner's details."""

    aoi_id: str
    geometry: MultiPolygon
