from dataclasses import dataclass
from typing import Callable

import geopandas as gpd

from pipelines.aois.aoi_ids import AoiIds


@dataclass(frozen=True)
class AoiSource:
    """An AOI dataset at a pinned version: how to load its features, and how
    each row's AOI id is derived from its (lowercased) attribute columns."""

    source: str
    version: str
    load: Callable[[], gpd.GeoDataFrame]
    aoi_ids: AoiIds
