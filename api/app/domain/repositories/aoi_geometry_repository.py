from abc import ABC, abstractmethod
from typing import List, Tuple

from shapely.geometry.base import BaseGeometry

# min longitude, min latitude, max longitude, max latitude
Bounds = Tuple[float, float, float, float]


class AoiGeometryRepository(ABC):
    @abstractmethod
    async def load(self, aoi) -> Tuple[List[BaseGeometry], List[float]]:
        """Return the geometries and areas (ha) of an AOI's features, in the
        order of its ids."""

    async def bounds(self, aoi) -> List[Bounds]:
        """Return the bounding box of each of an AOI's features, in the order of
        its ids. Sources that store bounding boxes can read them without
        loading geometries."""
        geometries, _ = await self.load(aoi)
        return [geometry.bounds for geometry in geometries]


class AoiNotFoundError(ValueError):
    """Raised when requested AOI ids don't exist in the geometry source."""
