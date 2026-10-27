from abc import ABC, abstractmethod
from typing import List, Optional, Sequence

from app.domain.models.aoi_match import AoiMatch


class AoiNameIndex(ABC):
    @abstractmethod
    async def search(
        self, text: str, aoi_types: Optional[Sequence[str]], limit: int
    ) -> List[AoiMatch]:
        """The best matches for `text`, best first, optionally only among the
        given AOI types."""
