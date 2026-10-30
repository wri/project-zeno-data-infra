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
    """Provide the AOIs of a zonal statistics run: their bounding boxes for
    planning, and their polygons tile by tile."""

    @property
    def aoi_type(self) -> str:
        """Return the value of the results' aoi_type column."""

    def read_bounds(self) -> pd.DataFrame:
        """Read every AOI's bounding box, without geometries."""

    def read(self, bounds: Bounds) -> gpd.GeoDataFrame:
        """Read the polygons of the AOIs whose bounding boxes intersect bounds."""

    @property
    def provenance(self) -> Dict[str, str]:
        """Where the polygons came from, for the results' metadata."""


@dataclass(frozen=True)
class IcebergSource:
    """Read AOIs from an Iceberg table, such as the API's custom AOI table. The
    catalog is configured through PYICEBERG_CATALOG__{CATALOG}__*
    environment variables on the client and every worker."""

    catalog: str
    table: str
    aoi_type: str
    row_filter: BooleanExpression = AlwaysTrue()

    def read_bounds(self) -> pd.DataFrame:
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

    @property
    def provenance(self) -> Dict[str, str]:
        return {"aoi_source": f"iceberg:{self.catalog}/{self.table}"}

    def _scan(self, row_filter: BooleanExpression, columns):
        # The table's current snapshot, loaded for each read: AOIs are assumed
        # not to change during a run. A table without snapshots (nothing saved
        # yet) scans as empty.
        table = load_catalog(self.catalog).load_table(self.table)
        return table.scan(row_filter=row_filter, selected_fields=columns)


def custom_aoi_source() -> IcebergSource:
    """Return the source of monitored custom AOIs, read from the table the API's
    custom AOI endpoints write."""
    return IcebergSource(
        catalog="aois",
        table="custom.custom_aois",
        aoi_type="custom",
        row_filter=EqualTo("monitor_status", "monitored"),
    )
