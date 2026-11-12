from abc import ABC, abstractmethod
from typing import List, Optional

from shapely.geometry.base import BaseGeometry

from app.domain.models.aoi_subregion import AoiSubregion


class AdminSubregions(ABC):
    @abstractmethod
    async def within(self, parent_id: Optional[str], level: int) -> List[AoiSubregion]:
        """The admin areas at a level below an admin area, or every country
        when there is no parent."""


class IntersectingSubregions(ABC):
    @abstractmethod
    async def within(self, geometry: BaseGeometry) -> List[AoiSubregion]:
        """The AOIs that lie inside a geometry. Areas that only share a border
        with it are left out."""
