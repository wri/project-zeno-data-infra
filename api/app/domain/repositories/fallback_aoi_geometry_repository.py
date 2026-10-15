import logging

from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository


class FallbackAoiGeometryRepository(AoiGeometryRepository):
    """Loads geometries from the primary repository, or from the fallback when
    the primary's dataset doesn't exist yet. Other errors, such as unknown
    ids, are raised as they are."""

    def __init__(self, primary: AoiGeometryRepository, fallback: AoiGeometryRepository):
        self.primary = primary
        self.fallback = fallback

    async def load(self, aoi):
        try:
            return await self.primary.load(aoi)
        except FileNotFoundError as error:
            logging.warning(
                {
                    "event": "aoi_geometry_fallback",
                    "aoi_type": aoi.type,
                    "missing_dataset": str(error),
                }
            )
            return await self.fallback.load(aoi)
