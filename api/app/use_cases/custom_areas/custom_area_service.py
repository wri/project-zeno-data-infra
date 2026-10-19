from typing import Dict

import shapely
from shapely.geometry import MultiPolygon, shape
from shapely.geometry.base import BaseGeometry

from app.analysis.common.geodesic_area import compute_geodesic_area_ha
from app.domain.models.custom_aoi import CustomAoi
from app.domain.repositories.custom_aoi_repository import CustomAoiRepository


class InvalidCustomAreaError(ValueError):
    pass


class CustomAreaService:
    def __init__(self, repository: CustomAoiRepository):
        self.repository = repository

    async def save(self, geometries: Dict[str, dict]) -> None:
        """Stores each GeoJSON geometry under its id as a valid 2D MultiPolygon.
        Nothing is stored if any geometry has no area once repaired."""
        repaired = {aoi_id: polygonal(shape(g)) for aoi_id, g in geometries.items()}
        without_area = [aoi_id for aoi_id, g in repaired.items() if g.is_empty]
        if without_area:
            raise InvalidCustomAreaError(f"Geometries without area: {without_area}")

        await self.repository.upsert(
            [
                CustomAoi(aoi_id, geometry, compute_geodesic_area_ha(geometry))
                for aoi_id, geometry in repaired.items()
            ]
        )

    async def delete(self, aoi_id: str) -> None:
        await self.repository.delete(aoi_id)


def polygonal(geometry: BaseGeometry) -> MultiPolygon:
    geometry = shapely.make_valid(shapely.force_2d(geometry))
    polygons = [
        polygon
        for part in shapely.get_parts(geometry)
        for polygon in shapely.get_parts(part)
        if polygon.geom_type == "Polygon"
    ]
    merged = shapely.union_all(polygons)
    if merged.geom_type == "Polygon":
        return MultiPolygon([merged])
    return merged if merged.geom_type == "MultiPolygon" else MultiPolygon()
