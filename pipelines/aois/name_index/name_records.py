from typing import Iterable, Mapping, Sequence

import pandas as pd

from pipelines.aois.name_index.external_importance import ExternalDetails
from pipelines.aois.name_index.namers.aoi_namer import AoiNamer, present


def name_records(
    aois: Mapping[str, pd.DataFrame],
    namers: Sequence[AoiNamer],
    external: ExternalDetails,
) -> pd.DataFrame:
    """One record per named AOI: its name, display name, alternate names from
    both its source and the external source, importance and area. AOIs the
    external source doesn't know fall back to their type's importance."""
    named = pd.concat(
        [
            namer.names(aois[namer.aoi_type]).assign(aoi_type=namer.aoi_type)
            for namer in namers
        ],
        ignore_index=True,
    )
    named = named[named["name"].notna()]
    external_names = (
        external.alternate_names.groupby(["aoi_type", "aoi_id"])["alternate_name"]
        .agg(list)
        .rename("external_names")
        .reset_index()
    )
    records = named.merge(external_names, how="left", on=["aoi_type", "aoi_id"])
    records = records.merge(
        external.importance[["aoi_type", "aoi_id", "importance"]],
        how="left",
        on=["aoi_type", "aoi_id"],
    )
    records["alternate_names"] = [
        distinct(own, outside)
        for own, outside in zip(records["alternate_names"], records["external_names"])
    ]
    records["importance"] = records["importance"].fillna(records["fallback_importance"])
    return records[
        [
            "aoi_type",
            "aoi_id",
            "name",
            "display_name",
            "alternate_names",
            "importance",
            "area_ha",
        ]
    ]


def distinct(*name_lists: Iterable[str] | float) -> list[str]:
    """Every non-blank name, trimmed and kept once, in order."""
    names: list[str] = []
    for name_list in name_lists:
        if not isinstance(name_list, list):
            continue
        for name in present(*name_list):
            if name.strip() not in names:
                names.append(name.strip())
    return names
