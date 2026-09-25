from typing import Dict

from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository


class ConcessionAoiGeometryRepository(AoiGeometryRepository):
    """Loads geometries from the repository registered for the concession
    type, since each type is stored as its own dataset."""

    def __init__(self, repositories: Dict[str, AoiGeometryRepository]):
        self.repositories = repositories

    async def load(self, aoi):
        return await self.repositories[aoi.concession_type].load(aoi)
