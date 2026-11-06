from typing import Callable

from app.analysis.common.geodesic_area import compute_geodesic_area_ha
from app.domain.repositories.aoi_geometry_repository import (
    AoiGeometryRepository,
    AoiNotFoundError,
)
from app.domain.repositories.custom_aoi_repository import CustomAoiRepository


class CustomAoiGeometryRepository(AoiGeometryRepository):
    """Geometries and geodesic areas of stored custom AOIs. The custom AOI store
    is created on first use, so other AOI types never need it."""

    def __init__(self, custom_aoi_repository: Callable[[], CustomAoiRepository]):
        self.custom_aoi_repository = custom_aoi_repository

    async def load(self, aoi):
        stored = await self.custom_aoi_repository().get(aoi.ids)
        found = {custom_aoi.aoi_id for custom_aoi in stored}
        missing = [aoi_id for aoi_id in aoi.ids if aoi_id not in found]
        if missing:
            raise AoiNotFoundError(f"AOI ids not found: {missing}")
        geometries = [custom_aoi.geometry for custom_aoi in stored]
        return geometries, [compute_geodesic_area_ha(g) for g in geometries]
