from abc import ABC, abstractmethod
from typing import List

from shapely.geometry.base import BaseGeometry

from app.domain.models.custom_aoi import CustomAoi


class CustomAoiRepository(ABC):
    @abstractmethod
    async def upsert(self, aois: List[CustomAoi]) -> None:
        """Store each AOI, replacing any already stored with the same id."""

    @abstractmethod
    async def delete(self, aoi_id: str) -> None:
        """Remove the AOI, if it exists."""

    @abstractmethod
    async def get(self, aoi_ids: List[str]) -> List[CustomAoi]:
        """The stored AOIs with these ids, in the order requested. Ids that
        aren't stored are left out."""

    @abstractmethod
    async def intersecting(self, geometry: BaseGeometry) -> List[CustomAoi]:
        """The stored AOIs whose geometry intersects this geometry."""
