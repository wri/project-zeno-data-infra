from typing import Any, Sequence

import pandas as pd
from pipelines.aois.name_index.namers.aoi_namer import AoiNamer, present, text


class GadmNames(AoiNamer):
    """Names each admin area by its own level's name, displayed with the
    names of the areas containing it, plus the whole world as the admin area
    "global". Disputed areas are left out.

    Admin areas down to level 2 fall back to being more important than
    protected areas, and smaller ones, mostly towns no outside source knows,
    to being less."""

    aoi_type = "admin"
    source = "gadm"

    def __init__(
        self,
        fallback_importance_by_level: Sequence[float] = (
            0.5,
            0.45,
            0.35,
            0.14,
            0.13,
            0.12,
        ),
        global_alternate_names: Sequence[str] = (),
        global_importance: float = 1.0,
    ):
        self.fallback_importance_by_level = fallback_importance_by_level
        self.global_alternate_names = global_alternate_names
        self.global_importance = global_importance

    def names(self, aois: pd.DataFrame) -> pd.DataFrame:
        undisputed = aois[~aois["is_disputed"].fillna(False).astype(bool)]
        records = [self.named(row) for row in undisputed.to_dict("records")]
        records.append(self.world(undisputed))
        return pd.DataFrame(
            records,
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
        level = int(row["adm_level"])
        names = [row.get(f"name_{each}") for each in range(level + 1)]
        return {
            "aoi_id": row["aoi_id"],
            "name": text(names[-1]),
            "display_name": ", ".join(present(*reversed(names))),
            "alternate_names": self.alternate_names(row, level),
            "fallback_importance": self.fallback_importance_by_level[level],
            "area_ha": row["area_ha"],
        }

    def world(self, aois: pd.DataFrame) -> dict[str, Any]:
        countries = aois[aois["adm_level"] == 0]
        return {
            "aoi_id": "global",
            "name": "Global",
            "display_name": "Global",
            "alternate_names": list(self.global_alternate_names),
            "fallback_importance": self.global_importance,
            "area_ha": countries["area_ha"].sum(),
        }

    def alternate_names(self, row: dict[str, Any], level: int) -> list[str]:
        """GADM separates variant names with "|" and writes "NA" for none."""
        variants = (text(row.get(f"varname_{level}")) or "").split("|")
        names = present(*variants, row.get(f"nl_name_{level}"))
        return [name for name in names if name != "NA"]
