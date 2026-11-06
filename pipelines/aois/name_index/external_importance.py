from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd


@dataclass(frozen=True)
class ExternalDetails:
    """What an outside source knows about AOIs: `importance` (`aoi_type`,
    `aoi_id`, `importance` from 0 to 1) for how well known each AOI is,
    `alternate_names` (`aoi_type`, `aoi_id`, `alternate_name`), and `sources`,
    a record of what they were taken from."""

    importance: pd.DataFrame
    alternate_names: pd.DataFrame
    sources: dict[str, Any]


class ExternalImportance(ABC):
    @abstractmethod
    def details(self, aois: Mapping[str, pd.DataFrame]) -> ExternalDetails:
        """Details for the AOIs of each type, given their attributes."""
