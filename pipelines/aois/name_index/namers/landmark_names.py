from typing import Any

from pipelines.aois.name_index.namers.aoi_namer import comma_joined, present
from pipelines.aois.name_index.namers.single_level_names import SingleLevelNames


class LandmarkNames(SingleLevelNames):
    """Indigenous and community lands, displayed with their people, category
    and country, with their people as an alternate name."""

    aoi_type = "indigenous_land"
    source = "landmark"
    name_column = "name"
    fallback_importance = 0.1

    def display_name(self, row: dict[str, Any]) -> str:
        return comma_joined(
            row["name"], row["ethncty_1"], row["category"], row["country"]
        )

    def alternate_names(self, row: dict[str, Any]) -> list[str]:
        return present(row["ethncty_1"])
