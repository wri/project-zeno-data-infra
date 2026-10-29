"""Where the polygons of areas of interest (AOIs) come from for zonal
statistics. A source is read in two ways: on the client, every AOI's bounding
box, to decide which tiles have work; and on each tile's Dask task, the
polygons whose bounding boxes intersect that tile. Sources keep their polygons
spatially grouped, so a tile's read touches only the files or row groups near
it."""

from dataclasses import dataclass
from typing import Dict, Protocol

import geopandas as gpd
import pandas as pd
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    GreaterThanOrEqual,
    LessThanOrEqual,
)

from pipelines.prefect_flows.tile_stages import Bounds


class AOISource(Protocol):
    @property
    def aoi_type(self) -> str:
        """Value of the results' aoi_type column."""

    def bounds(self) -> pd.DataFrame:
        """aoi_id, xmin, ymin, xmax, ymax of every AOI, without geometries."""

    def read(self, bounds: Bounds) -> gpd.GeoDataFrame:
        """aoi_id and geometry (EPSG:4326) of the AOIs whose bounding boxes
        intersect bounds (west, south, east, north)."""

    def provenance(self) -> Dict[str, str]:
        """Where the polygons came from, for the results' metadata."""


@dataclass(frozen=True)
class IcebergSource:
    """AOIs in an Iceberg table with aoi_id, WKB geometry and xmin, ymin, xmax,
    ymax columns, such as the custom AOI table the API writes (partitioned by S2
    cell, so a bounding box filter prunes to the files near it). The catalog is
    configured through PYICEBERG_CATALOG__{CATALOG}__* environment variables, on
    the client and on every worker."""

    catalog: str
    table: str
    aoi_type: str
    row_filter: BooleanExpression = AlwaysTrue()

    def bounds(self) -> pd.DataFrame:
        columns = ("aoi_id", "xmin", "ymin", "xmax", "ymax")
        return self._scan(self.row_filter, columns).to_pandas()

    def read(self, bounds: Bounds) -> gpd.GeoDataFrame:
        west, south, east, north = bounds
        overlaps = And(
            LessThanOrEqual("xmin", east),
            GreaterThanOrEqual("xmax", west),
            LessThanOrEqual("ymin", north),
            GreaterThanOrEqual("ymax", south),
        )
        rows = self._scan(
            And(self.row_filter, overlaps), ("aoi_id", "geometry")
        ).to_pandas()
        return gpd.GeoDataFrame(
            {"aoi_id": rows["aoi_id"]},
            geometry=gpd.GeoSeries.from_wkb(rows["geometry"]),
            crs="EPSG:4326",
        )

    def provenance(self) -> Dict[str, str]:
        return {"aoi_source": f"iceberg:{self.catalog}/{self.table}"}

    def _scan(self, row_filter: BooleanExpression, columns):
        # The table's current snapshot, loaded for each read: AOIs are assumed
        # not to change during a run. A table without snapshots (nothing saved
        # yet) scans as empty.
        table = load_catalog(self.catalog).load_table(self.table)
        return table.scan(row_filter=row_filter, selected_fields=columns)


def custom_aois() -> IcebergSource:
    """Monitored custom AOIs, from the table the API's custom AOI endpoints
    write."""
    return IcebergSource(
        catalog="aois",
        table="custom.custom_aois",
        aoi_type="custom",
        row_filter=EqualTo("monitor_status", "monitored"),
    )
