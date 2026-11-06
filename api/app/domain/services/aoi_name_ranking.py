from typing import Dict, List, Optional, Sequence

from app.domain.models.aoi_match import AoiMatch
from app.domain.models.aoi_search import AoiCandidate, MatchKind


def default_match_scores() -> Dict[MatchKind, float]:
    """How well each kind of match matches, from 0 to 1. An AOI's own name
    counts for a little more than its alternate names."""
    return {
        MatchKind.EXACT_NAME: 1.0,
        MatchKind.EXACT_ALTERNATE: 0.9,
        MatchKind.NAME_PREFIX: 0.75,
        MatchKind.ALTERNATE_PREFIX: 0.7,
        MatchKind.NAME_WORDS: 0.6,
        MatchKind.ALTERNATE_WORDS: 0.55,
    }


class AoiNameRanking:
    """Scores each AOI from how well its name matches, whether it lies within
    the containing place searched for, and its importance, so a well-known
    place can outrank a better spelled obscure one. A user's custom AOIs count
    as most important. Each AOI ranks by its best match, and ties go to the
    larger area."""

    def __init__(
        self,
        match_scores: Optional[Dict[MatchKind, float]] = None,
        corrected_match_weight: float = 0.8,
        match_weight: float = 0.5,
        containing_place_weight: float = 0.2,
        importance_weight: float = 0.3,
        custom_aoi_importance: float = 1.0,
    ):
        self.match_scores = match_scores or default_match_scores()
        self.corrected_match_weight = corrected_match_weight
        self.match_weight = match_weight
        self.containing_place_weight = containing_place_weight
        self.importance_weight = importance_weight
        self.custom_aoi_importance = custom_aoi_importance

    def best(self, candidates: Sequence[AoiCandidate], limit: int) -> List[AoiMatch]:
        best: Dict[AoiMatch, AoiCandidate] = {}
        for candidate in candidates:
            current = best.get(candidate.aoi)
            if current is None or self.score(candidate) > self.score(current):
                best[candidate.aoi] = candidate
        ranked = sorted(
            best.values(),
            key=lambda candidate: (self.score(candidate), candidate.area_ha),
            reverse=True,
        )
        return [candidate.aoi for candidate in ranked[:limit]]

    def score(self, candidate: AoiCandidate) -> float:
        match = self.match_scores[candidate.match.kind]
        if candidate.match.corrected:
            match *= self.corrected_match_weight
        return (
            self.match_weight * match
            + self.containing_place_weight * candidate.within_containing_place
            + self.importance_weight * self.importance(candidate)
        )

    def importance(self, candidate: AoiCandidate) -> float:
        if candidate.aoi.aoi_type == "custom_aoi":
            return self.custom_aoi_importance
        return candidate.importance
