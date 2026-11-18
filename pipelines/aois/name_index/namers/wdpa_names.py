from typing import Any

from pipelines.aois.name_index.namers.aoi_namer import comma_joined, present
from pipelines.aois.name_index.namers.single_level_names import SingleLevelNames


class WdpaNames(SingleLevelNames):
    """Protected areas, displayed with their designation and country, with
    their English name as an alternate. They fall back to being less
    important than larger admin areas but more than smaller ones."""

    aoi_type = "protected_area"
    source = "wdpa"
    name_column = "name"
    fallback_importance = 0.15

    def display_name(self, row: dict[str, Any]) -> str:
        return comma_joined(row["name"], row["desig_eng"], row["iso3"])

    def alternate_names(self, row: dict[str, Any]) -> list[str]:
        return present(row["name_eng"])
