from typing import Dict

from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository


class AoiTypeRoutingGeometryRepository(AoiGeometryRepository):
    """Loads geometries from the repository registered for the AOI's type."""

    def __init__(self, repositories: Dict[str, AoiGeometryRepository]):
        self.repositories = repositories

    async def load(self, aoi):
        if aoi.type not in self.repositories:
            raise ValueError(f"No geometry source for AOI type {aoi.type}.")
        return await self.repositories[aoi.type].load(aoi)
