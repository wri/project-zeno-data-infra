from abc import abstractmethod
from typing import Any

import pandas as pd

from pipelines.aois.name_index.namers.aoi_namer import AoiNamer, text


class SingleLevelNames(AoiNamer):
    """Names AOIs whose `name_column` gives each AOI's name, with one
    fallback importance for all of them."""

    name_column: str
    fallback_importance: float

    def names(self, aois: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            [self.named(row) for row in aois.to_dict("records")],
            columns=[
                "aoi_id",
                "name",
                "display_name",
                "alternate_names",
                "fallback_importance",
                "area_ha",
            ],
        )

    def named(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "aoi_id": row["aoi_id"],
            "name": text(row[self.name_column]),
            "display_name": self.display_name(row),
            "alternate_names": self.alternate_names(row),
            "fallback_importance": self.fallback_importance,
            "area_ha": row["area_ha"],
        }

    @abstractmethod
    def display_name(self, row: dict[str, Any]) -> str:
        """The name with what tells it apart from others of the same name."""

    @abstractmethod
    def alternate_names(self, row: dict[str, Any]) -> list[str]:
        """Other names the AOI is known by."""
