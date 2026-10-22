from abc import ABC, abstractmethod

import geopandas as gpd
import pandas as pd


class AoiSource(ABC):
    """An AOI dataset at a pinned version: how to load it, how to derive each
    feature's AOI id from its (lowercased) attributes, and which (lowercased)
    attributes to leave out of the output."""

    source: str
    version: str
    dropped_columns: tuple[str, ...]

    @abstractmethod
    def load(self) -> gpd.GeoDataFrame: ...

    @abstractmethod
    def aoi_ids(self, attributes: pd.DataFrame) -> pd.Series: ...
