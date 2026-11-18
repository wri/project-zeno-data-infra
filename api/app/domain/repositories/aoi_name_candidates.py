from abc import ABC, abstractmethod
from typing import List, Sequence

from app.domain.models.aoi_search import AoiCandidate, AoiSearch
from app.domain.models.custom_aoi import CustomAoiName


class AoiNameCandidates(ABC):
    @abstractmethod
    async def candidates(
        self, search: AoiSearch, custom_aois: Sequence[CustomAoiName] = ()
    ) -> List[AoiCandidate]:
        """AOIs of the searched types whose names match the place searched for,
        ignoring case and accents, along with the given custom AOIs that match.

        For each kind of match, returns at least the most important
        `search.limit` AOIs, both among all AOIs and among those within the
        containing place, so that no AOI ranking in the top `search.limit` is
        missed. An AOI is within the containing place when its display name has
        every word of it, or when its name matches the whole search text.

        A word starting no known word is corrected to the nearest known words,
        within one edit per four letters, and matches found that way are marked
        as corrected. An AOI may be returned once for each way it matches."""
