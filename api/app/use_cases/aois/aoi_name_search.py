from typing import List, Optional, Sequence

from app.domain.models.aoi_match import AoiMatch
from app.domain.models.aoi_search import AoiSearch
from app.domain.models.custom_aoi import CustomAoiName
from app.domain.repositories.aoi_name_candidates import AoiNameCandidates
from app.domain.services.aoi_name_ranking import AoiNameRanking


class AoiNameSearch:
    """Finds the AOIs whose names best match a search text, best first,
    optionally only among the given AOI types. A user's custom AOIs are searched
    as type `custom_aoi`, ranked above other AOIs whose names match as well."""

    def __init__(self, candidates: AoiNameCandidates, ranking: AoiNameRanking):
        self.candidates = candidates
        self.ranking = ranking

    async def search(
        self,
        text: str,
        aoi_types: Optional[Sequence[str]],
        limit: int,
        custom_aois: Sequence[CustomAoiName] = (),
    ) -> List[AoiMatch]:
        search = AoiSearch.parse(text, aoi_types, limit)
        if not search.place:
            return []
        candidates = await self.candidates.candidates(search, custom_aois)
        return self.ranking.best(candidates, limit)
