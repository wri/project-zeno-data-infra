from abc import ABC, abstractmethod
from typing import List

from app.domain.models.custom_aoi import CustomAoi


class CustomAoiRepository(ABC):
    @abstractmethod
    async def upsert(self, aois: List[CustomAoi]) -> None:
        """Store each AOI, replacing any already stored with the same id."""

    @abstractmethod
    async def delete(self, aoi_id: str) -> None:
        """Remove the AOI, if it exists."""
