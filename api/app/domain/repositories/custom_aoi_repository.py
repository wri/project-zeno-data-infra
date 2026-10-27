from abc import ABC, abstractmethod
from typing import List

from shapely.geometry.base import BaseGeometry

from app.domain.models.custom_aoi import CustomAoi, CustomAoiGeometry, CustomAoiName


class CustomAoiRepository(ABC):
    @abstractmethod
    async def upsert(self, aois: List[CustomAoi]) -> None:
        """Store each AOI, replacing any already stored with the same id."""

    @abstractmethod
    async def delete(self, aoi_id: str) -> None:
        """Remove the AOI, if it exists."""

    @abstractmethod
    async def get(self, aoi_ids: List[str]) -> List[CustomAoiGeometry]:
        """The geometries of the stored AOIs with these ids, in the order
        requested. Ids that aren't stored are left out."""

    @abstractmethod
    async def intersecting(self, geometry: BaseGeometry) -> List[CustomAoiGeometry]:
        """The geometries of the stored AOIs that intersect this geometry."""

    @abstractmethod
    async def names_owned_by(self, user_id: str) -> List[CustomAoiName]:
        """The ids and names of the AOIs this user owns."""
