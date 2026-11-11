from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Set

from app.models.common.areas_of_interest import AreaOfInterest


class PrecomputedCoverage(ABC):
    """Decides which AOI ids a precomputed results table can serve."""

    @abstractmethod
    async def select_precomputed_ids(self, aoi: AreaOfInterest) -> Set[str]:
        """Select which of the AOI's ids the table holds statistics for."""


class PrecomputedAdminLevels(PrecomputedCoverage):
    """Coverage of a table built for every admin area down to the deepest
    precomputed level."""

    async def select_precomputed_ids(self, aoi: AreaOfInterest) -> Set[str]:
        # A request's admin areas are all above or all below that level.
        return set(aoi.ids) if aoi.has_precomputed_results() else set()


@dataclass(frozen=True)
class PrecomputedSource:
    """A precomputed results table that serves one AOI type.

    query_service: runs SQL against the table, which the SQL names `data_source`.
    coverage: decides which requested ids the table can serve.
    """

    query_service: Any
    coverage: PrecomputedCoverage


def sql_string_list(values: Iterable[str]) -> str:
    """Format values as a comma-separated list of SQL string literals."""
    # Ids come from requests, so quotes are doubled to keep them inside literals.
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)
