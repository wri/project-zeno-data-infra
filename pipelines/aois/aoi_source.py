from dataclasses import dataclass
from typing import Callable

import geopandas as gpd
import pandas as pd

AoiIds = Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class AoiSource:
    """An AOI dataset at a pinned version, how to load it, and how to derive
    each feature's AOI id from its (lowercased) attributes."""

    source: str
    version: str
    load: Callable[[], gpd.GeoDataFrame]
    aoi_ids: AoiIds


def column_ids(column: str) -> AoiIds:
    def ids(attributes: pd.DataFrame) -> pd.Series:
        values = attributes[column]
        if pd.api.types.is_float_dtype(values):
            # integer ids are read as floats when some are missing (101 -> 101.0)
            values = values.astype("Int64")
        return values.astype("string")

    return ids
