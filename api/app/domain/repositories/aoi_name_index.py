from abc import ABC, abstractmethod
from typing import List, Optional, Sequence

from app.domain.models.aoi_match import AoiMatch
from app.domain.models.custom_aoi import CustomAoiName


class AoiNameIndex(ABC):
    @abstractmethod
    async def search(
        self,
        text: str,
        aoi_types: Optional[Sequence[str]],
        limit: int,
        custom_aois: Sequence[CustomAoiName] = (),
    ) -> List[AoiMatch]:
        """The best matches for `text`, best first, optionally only among the
        given AOI types. Custom AOIs are searched as type `custom_aoi`, ranked
        above other AOIs whose names match as well."""
