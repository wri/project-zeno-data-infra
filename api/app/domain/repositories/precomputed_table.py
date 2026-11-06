from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, List, Set


class PrecomputedCoverage(ABC):
    """Decides which AOI ids a precomputed results table can serve."""

    @abstractmethod
    async def select_covered_ids(self, aoi_ids: List[str]) -> Set[str]:
        """Select the ids whose statistics the table holds."""


class EveryAoiCovered(PrecomputedCoverage):
    """Coverage of a table built for every AOI of its type."""

    async def select_covered_ids(self, aoi_ids: List[str]) -> Set[str]:
        return set(aoi_ids)


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
