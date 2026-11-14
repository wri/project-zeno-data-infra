from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence

from app.domain.models.aoi_match import AoiMatch


@dataclass(frozen=True)
class AoiSearch:
    """A search for AOIs by name. Text after the first comma names a place
    containing the one being looked for, as in "Lebanon, Pennsylvania"."""

    text: str
    place: str
    containing_place: str
    aoi_types: Optional[Sequence[str]]
    limit: int

    @classmethod
    def parse(
        cls, text: str, aoi_types: Optional[Sequence[str]], limit: int
    ) -> "AoiSearch":
        place, _, containing_place = text.partition(",")
        return cls(
            text=text.strip(),
            place=place.strip(),
            containing_place=containing_place.strip(),
            aoi_types=aoi_types,
            limit=limit,
        )


class MatchKind(Enum):
    """How an AOI's name or one of its alternate names matches the place
    searched for: equal to it, starting with it, or with a word starting with
    each of its words."""

    EXACT_NAME = "exact_name"
    EXACT_ALTERNATE = "exact_alternate"
    NAME_PREFIX = "name_prefix"
    ALTERNATE_PREFIX = "alternate_prefix"
    NAME_WORDS = "name_words"
    ALTERNATE_WORDS = "alternate_words"


@dataclass(frozen=True)
class NameMatch:
    """How a name matched, and whether any word searched for had to be
    corrected to a known word first."""

    kind: MatchKind
    corrected: bool = False


@dataclass(frozen=True)
class AoiCandidate:
    """An AOI matching a search, with what ranking it needs."""

    aoi: AoiMatch
    match: NameMatch
    within_containing_place: bool
    importance: float
    area_ha: float
