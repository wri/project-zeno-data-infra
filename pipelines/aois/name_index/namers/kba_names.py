from typing import Any

from pipelines.aois.name_index.namers.aoi_namer import comma_joined, present
from pipelines.aois.name_index.namers.single_level_names import SingleLevelNames


class KbaNames(SingleLevelNames):
    """Key biodiversity areas, by national name, with their international
    name as an alternate."""

    aoi_type = "key_biodiversity_area"
    source = "kba"
    name_column = "natname"
    fallback_importance = 0.1

    def display_name(self, row: dict[str, Any]) -> str:
        return comma_joined(row["natname"], row["country"])

    def alternate_names(self, row: dict[str, Any]) -> list[str]:
        return present(row["intname"])
