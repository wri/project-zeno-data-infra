from typing import Dict

from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository


class AoiTypeRoutingGeometryRepository(AoiGeometryRepository):
    """Loads geometries from the repository registered for the AOI's type."""

    def __init__(self, repositories: Dict[str, AoiGeometryRepository]):
        self.repositories = repositories

    async def load(self, aoi):
        return await self.repository_for(aoi).load(aoi)

    async def bounds(self, aoi):
        return await self.repository_for(aoi).bounds(aoi)

    def repository_for(self, aoi) -> AoiGeometryRepository:
        if aoi.type not in self.repositories:
            raise ValueError(f"No geometry source for AOI type {aoi.type}.")
        return self.repositories[aoi.type]
