import shapely
from shapely.geometry import MultiPolygon, shape
from shapely.geometry.base import BaseGeometry

from app.analysis.common.geodesic_area import compute_geodesic_area_ha
from app.domain.models.custom_aoi import CustomAoi
from app.domain.repositories.custom_aoi_repository import CustomAoiRepository
from app.models.custom_aois import CustomAoiIn, CustomAoisIn


class InvalidCustomAoiError(ValueError):
    pass


class CustomAoiService:
    def __init__(self, repository: CustomAoiRepository):
        self.repository = repository

    async def save(self, custom_aois: CustomAoisIn) -> None:
        """Stores each AOI under its id, with its geometry as a valid 2D
        MultiPolygon. Nothing is stored if any geometry has no area once
        repaired."""
        aois = [custom_aoi(aoi) for aoi in custom_aois.aois]
        without_area = [aoi.aoi_id for aoi in aois if aoi.geometry.is_empty]
        if without_area:
            raise InvalidCustomAoiError(f"Geometries without area: {without_area}")

        await self.repository.upsert(aois)

    async def delete(self, aoi_id: str) -> None:
        await self.repository.delete(aoi_id)


def custom_aoi(aoi: CustomAoiIn) -> CustomAoi:
    geometry = polygonal(shape(aoi.geometry))
    return CustomAoi(
        aoi_id=aoi.id,
        geometry=geometry,
        area_ha=compute_geodesic_area_ha(geometry),
        name=aoi.name,
        user_id=aoi.user_id,
    )


def polygonal(geometry: BaseGeometry) -> MultiPolygon:
    """A valid 2D MultiPolygon, without any parts that collapse to lines or
    points when repaired. Empty if nothing with area is left."""
    valid = shapely.make_valid(
        shapely.force_2d(geometry), method="structure", keep_collapsed=False
    )
    if valid.is_empty:
        return MultiPolygon()
    if valid.geom_type == "Polygon":
        return MultiPolygon([valid])
    return valid
