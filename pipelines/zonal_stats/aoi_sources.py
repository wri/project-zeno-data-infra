"""Where the polygons of areas of interest (AOIs) come from for zonal
statistics. A source is read in two ways: on the client, every AOI's bounding
box, to decide which tiles have work; and on each tile's Dask task, the
polygons of the AOIs that tile was assigned. Sources filter their files or row
groups by bounding box and aoi_id statistics, so a tile's read skips the ones
that can't hold its AOIs."""

from dataclasses import dataclass, replace
from typing import Dict, List, Optional, Protocol

import fsspec
import geopandas as gpd
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    GreaterThanOrEqual,
    In,
    LessThanOrEqual,
)

from pipelines.aois.sources import aoi_sources
from pipelines.globals import ANALYTICS_BUCKET
from pipelines.prefect_flows.tile_stages import Bounds

# The API's AOI type of each AOI dataset the pipeline builds
AOI_TYPES = {
    "gadm": "admin",
    "wdpa": "protected_area",
    "kba": "key_biodiversity_area",
    "landmark": "indigenous_land",
}


class AOISource(Protocol):
    """Provide the AOIs of a zonal statistics run: their bounding boxes for
    planning, and their polygons tile by tile."""

    @property
    def aoi_type(self) -> str:
        """Return the value of the results' aoi_type column."""

    def read_bounds(self) -> pd.DataFrame:
        """Read the bounding box of every AOI that counts, without geometries;
        these define the run's AOIs."""

    def read(self, bounds: Bounds, aoi_ids: List[str]) -> gpd.GeoDataFrame:
        """Read the polygons of the AOIs among aoi_ids whose bounding boxes
        intersect bounds."""

    @property
    def provenance(self) -> Dict[str, str]:
        """Where the polygons came from, for the results' metadata."""


@dataclass(frozen=True)
class GeoParquetSource:
    """Read AOIs from a GeoParquet with aoi_id, geometry and a covering bbox,
    such as the AOI pipeline's aois.parquet."""

    uri: str
    aoi_type: str

    def read_bounds(self) -> pd.DataFrame:
        filesystem, path = fsspec.core.url_to_fs(self.uri)
        table = pq.read_table(path, filesystem=filesystem, columns=["aoi_id", "bbox"])
        bounds = table.flatten().to_pandas()
        return bounds.rename(columns=lambda name: name.removeprefix("bbox."))

    def read(self, bounds: Bounds, aoi_ids: List[str]) -> gpd.GeoDataFrame:
        return gpd.read_parquet(
            self.uri,
            columns=["aoi_id", "geometry"],
            bbox=bounds,
            filters=pc.field("aoi_id").isin(aoi_ids),
        )

    def read_attributes(self, columns: List[str]) -> pd.DataFrame:
        """Read columns of every AOI, without geometries."""
        return pd.read_parquet(self.uri, columns=columns)

    @property
    def provenance(self) -> Dict[str, str]:
        return {"aoi_source": self.uri}


@dataclass(frozen=True)
class IcebergSource:
    """Read AOIs from an Iceberg table, such as the API's custom AOI table. Reads
    use one pinned snapshot, so every read of a run sees the same AOIs even if
    the table changes meanwhile; pin() before reading. The catalog is configured
    through PYICEBERG_CATALOG__{CATALOG}__* environment variables on the client
    and every worker."""

    catalog: str
    table: str
    aoi_type: str
    pinned: bool = False
    snapshot_id: Optional[int] = None  # None when pinned to an empty table

    def pin(self) -> "IcebergSource":
        """Return this source fixed at the table's current snapshot."""
        if self.pinned:
            return self
        snapshot = load_catalog(self.catalog).load_table(self.table).current_snapshot()
        return replace(self, pinned=True, snapshot_id=snapshot and snapshot.snapshot_id)

    def read_bounds(self) -> pd.DataFrame:
        columns = ("aoi_id", "xmin", "ymin", "xmax", "ymax")
        return self._scan(AlwaysTrue(), columns).to_pandas()

    def read(self, bounds: Bounds, aoi_ids: List[str]) -> gpd.GeoDataFrame:
        west, south, east, north = bounds
        overlaps = And(
            LessThanOrEqual("xmin", east),
            GreaterThanOrEqual("xmax", west),
            LessThanOrEqual("ymin", north),
            GreaterThanOrEqual("ymax", south),
        )
        rows = self._scan(
            And(overlaps, In("aoi_id", aoi_ids)), ("aoi_id", "geometry")
        ).to_pandas()
        return gpd.GeoDataFrame(
            {"aoi_id": rows["aoi_id"]},
            geometry=gpd.GeoSeries.from_wkb(rows["geometry"]),
            crs="EPSG:4326",
        )

    @property
    def provenance(self) -> Dict[str, str]:
        return {
            "aoi_source": f"iceberg:{self.catalog}/{self.table}",
            "aoi_snapshot_id": str(self.snapshot_id),
        }

    def _scan(self, row_filter: BooleanExpression, columns):
        if not self.pinned:
            raise RuntimeError(
                f"Pin {self.table} (pin()) before reading it, so every read of a "
                "run sees the same snapshot"
            )
        # A table without snapshots (nothing saved yet) scans as empty.
        table = load_catalog(self.catalog).load_table(self.table)
        return table.scan(
            row_filter=row_filter, selected_fields=columns, snapshot_id=self.snapshot_id
        )


def custom_aoi_source() -> IcebergSource:
    """Return the source of custom AOIs, pinned to the current snapshot of the
    table the API's custom AOI endpoints write."""
    source = IcebergSource(
        catalog="aois", table="custom.custom_aois", aoi_type="custom"
    )
    return source.pin()


def predefined_aoi_sources() -> List[GeoParquetSource]:
    """Return a source for every AOI dataset the AOI pipeline builds, read from
    its GeoParquet."""
    return [
        GeoParquetSource(
            f"s3://{ANALYTICS_BUCKET}/vectors/aois/{source.source}/{source.version}"
            "/aois.parquet",
            AOI_TYPES[source.source],
        )
        for source in aoi_sources()
    ]
