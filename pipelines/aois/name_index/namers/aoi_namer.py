from abc import ABC, abstractmethod
from typing import Any, Optional

import pandas as pd


class AoiNamer(ABC):
    """Names the AOIs of one type from the attributes of its pipeline source."""

    aoi_type: str
    source: str

    @abstractmethod
    def names(self, aois: pd.DataFrame) -> pd.DataFrame:
        """For each searchable AOI: `aoi_id`, `name`, `display_name`,
        `alternate_names` (a list), `fallback_importance` (0 to 1, used when
        no outside source knows the AOI) and `area_ha`. AOIs without a name
        are left out of search."""


def text(value: Any) -> Optional[str]:
    """The value if it's non-empty text, else None."""
    if isinstance(value, str) and value.strip():
        return value
    return None


def comma_joined(*parts: Any) -> str:
    """The non-empty parts, separated by commas."""
    return ", ".join(part for part in map(text, parts) if part is not None)


def present(*values: Any) -> list[str]:
    """The values that are non-empty text."""
    return [value for value in map(text, values) if value is not None]
