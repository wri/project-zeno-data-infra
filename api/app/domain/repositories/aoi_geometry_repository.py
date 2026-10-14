from abc import ABC, abstractmethod
from typing import List, Tuple

from shapely.geometry.base import BaseGeometry


class AoiGeometryRepository(ABC):
    @abstractmethod
    async def load(self, aoi) -> Tuple[List[BaseGeometry], List[float]]:
        """Return the geometries and areas (ha) of an AOI's features, in the
        order of its ids."""
