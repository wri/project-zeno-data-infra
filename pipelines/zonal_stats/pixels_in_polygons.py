"""Zonal statistics of vector areas of interest (AOIs) over a raster grid,
computed by testing the centers of only the pixels that count against the AOI
polygons, rather than rasterizing the polygons onto the grid.

The grid is split into tiles (one per zarr chunk with data), and only tiles that
some AOI's bounding box touches get a Dask task. Each task:
1. reads the polygons of the AOIs near its tile from the AOI source and clips
   them to the tile;
2. asks the dataset for the tile's candidate pixels: pixel-center x
   and y, the columns results are grouped by (e.g. date, confidence, class) and
   the column to sum (e.g. area);
3. for each distinct polygon, keeps the pixels whose centers it contains
   (a shapely STRtree "contains" query, the API's pixel-center rule) and sums
   that column per group;
4. writes these partial sums (aoi_id, groups, sum) to a part file.

An AOI spanning several tiles has partial sums in each. A second round of tasks,
one per range of aoi_ids, adds up each AOI's partial sums from every part and
writes a bucket file. The client appends the buckets, already in aoi_id order,
into the result. No process holds every pixel or every result.
"""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import fsspec
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import shapely
from shapely.geometry.base import BaseGeometry

from pipelines.prefect_flows.tile_stages import Bounds, map_tiles
from pipelines.zonal_stats.aoi_sources import AOISource

# (row index range, column index range, bounds) of one tile of the grid
Tile = Tuple[slice, slice, Bounds]
# (row index range, column index range) -> the tile's candidate pixels as
# pixels: pixel-center x and y, the columns sums are grouped by and the column
# that is summed
TilePixels = Callable[[slice, slice], pd.DataFrame]

AOIS_PER_BUCKET = 20_000
ROW_GROUP_SIZE = 100_000


@dataclass(frozen=True)
class TileTask:
    """Describe the work of one tile's Dask task (tile_sums)."""

    ys: slice
    """Row index range of the tile in the grid (positions for isel, not y
    coordinates), for reading its pixels."""
    xs: slice
    """Column index range of the tile in the grid (positions for isel, not x
    coordinates), for reading its pixels."""
    bounds: Bounds
    """(west, south, east, north) of the tile at pixel edges, for reading and
    clipping AOI polygons."""
    aoi_source: AOISource
    """Where the AOI polygons come from."""
    read_pixels: TilePixels
    """The dataset's reader of the tile's candidate pixels."""
    by: List[str]
    """Names of the pixel columns the sums are grouped by."""
    sum_column: str
    """Name of the pixel column that is summed."""
    part_uri: str
    """File the tile's partial sums are written to."""


@dataclass(frozen=True)
class BucketTask:
    """Describe the work of one bucket's Dask task (combine_tile_sums)."""

    position: int
    """Order of the bucket in the result."""
    first_id: str
    """Smallest aoi_id in the bucket."""
    last_id: str
    """Largest aoi_id in the bucket."""
    part_uris: List[str]
    """Every tile's part file, read filtered to the bucket's aoi_ids."""
    by: List[str]
    """Names of the columns the sums are grouped by."""
    sum_column: str
    """Name of the column that is summed."""
    finish: Callable[[pd.DataFrame], pd.DataFrame]
    """The dataset's last step on an AOI's summed rows: labels, columns, order."""
    bucket_uri: str
    """File the bucket's results are written to."""


def sum_pixels_in_polygons(
    pixels: pd.DataFrame,
    polygons: Sequence[BaseGeometry],
    by: List[str],
    sum_column: str,
) -> pd.DataFrame:
    """Sum a column over the pixels inside each polygon, by group, keyed by the
    polygon's position."""
    # Every (polygon, pixel) pair where the polygon contains the pixel center,
    # from a spatial index of the pixels: bounding boxes first, then the exact
    # test, in one vectorized call.
    tree = shapely.STRtree(shapely.points(pixels["x"], pixels["y"]))
    polygon, pixel = tree.query(polygons, predicate="contains")
    return (
        pixels.iloc[pixel][[*by, sum_column]]
        .assign(polygon=polygon)
        .groupby(["polygon", *by], as_index=False)[sum_column]
        .sum()
    )


def tile_sums(tile: TileTask) -> Optional[str]:
    """Write one tile's partial sums per AOI (steps 1-4 above) to a part file,
    as a Dask task. Returns the part's URI, or None if nothing in the tile
    is covered."""
    aois = tile.aoi_source.read(tile.bounds)
    aois["geometry"] = aois.geometry.clip_by_rect(*tile.bounds)
    aois = aois[~aois.geometry.is_empty]
    if aois.empty:
        return None
    pixels = tile.read_pixels(tile.ys, tile.xs)
    if pixels.empty:
        return None
    # AOIs with identical polygons (e.g. sites listed twice) are covered once.
    polygon_of_aoi, polygons = pd.factorize(aois.geometry.to_wkb())
    sums = sum_pixels_in_polygons(
        pixels, gpd.GeoSeries.from_wkb(polygons), tile.by, tile.sum_column
    )
    if sums.empty:
        return None
    aoi_polygons = pd.DataFrame(
        {"aoi_id": aois["aoi_id"].to_numpy(), "polygon": polygon_of_aoi}
    )
    sums = aoi_polygons.merge(sums)[["aoi_id", *tile.by, tile.sum_column]].sort_values(
        ["aoi_id", *tile.by], ignore_index=True
    )
    with fsspec.open(tile.part_uri, "wb", auto_mkdir=True) as f:
        sums.to_parquet(f, index=False)
    return tile.part_uri


def combine_tile_sums(bucket: BucketTask) -> Tuple[int, Optional[str]]:
    """Add up a bucket of AOIs' partial sums from every part, finish them and
    write them to a bucket file, as a Dask task. Returns the bucket's
    position and URI, or None if none of its AOIs is covered."""
    filesystem, _ = fsspec.core.url_to_fs(bucket.bucket_uri)
    paths = [fsspec.core.url_to_fs(uri)[1] for uri in bucket.part_uris]
    in_bucket = (pc.field("aoi_id") >= bucket.first_id) & (
        pc.field("aoi_id") <= bucket.last_id
    )
    sums = (
        ds.dataset(paths, filesystem=filesystem, format="parquet")
        .to_table(filter=in_bucket)
        .to_pandas()
    )
    if sums.empty:
        return bucket.position, None
    sums = sums.groupby(["aoi_id", *bucket.by], as_index=False)[bucket.sum_column].sum()
    with fsspec.open(bucket.bucket_uri, "wb", auto_mkdir=True) as f:
        bucket.finish(sums).to_parquet(f, index=False)
    return bucket.position, bucket.bucket_uri


def zonal_stats(
    aoi_source: AOISource,
    tiles: List[Tile],
    read_pixels: TilePixels,
    by: List[str],
    sum_column: str,
    finish: Callable[[pd.DataFrame], pd.DataFrame],
    schema: pa.Schema,
    result_uri: str,
    metadata: Dict[str, str],
) -> Dict:
    """Compute zonal statistics of every AOI in aoi_source over the tiles and
    write them to result_uri. Returns a summary of the run."""
    aoi_bounds = aoi_source.bounds()
    tiles_with_aois = _tiles_with_aois(tiles, aoi_bounds)
    parts_uri = result_uri.removesuffix(".parquet") + "_parts"
    print(
        f"{aoi_source.aoi_type} zonal stats: {len(aoi_bounds):,} AOIs, "
        f"{len(tiles_with_aois):,} of {len(tiles):,} tiles"
    )

    tile_tasks = [
        TileTask(
            ys=ys,
            xs=xs,
            bounds=bounds,
            aoi_source=aoi_source,
            read_pixels=read_pixels,
            by=by,
            sum_column=sum_column,
            part_uri=f"{parts_uri}/tiles/{ys.start}_{xs.start}.parquet",
        )
        for ys, xs, bounds in tiles_with_aois
    ]
    part_uris = [uri for uri in map_tiles(tile_sums, tile_tasks, "tiles") if uri]

    aoi_ids = np.sort(aoi_bounds["aoi_id"].unique())
    bucket_tasks = (
        [
            BucketTask(
                position=i,
                first_id=ids[0],
                last_id=ids[-1],
                part_uris=part_uris,
                by=by,
                sum_column=sum_column,
                finish=finish,
                bucket_uri=f"{parts_uri}/buckets/{i}.parquet",
            )
            for i, ids in enumerate(_buckets(aoi_ids))
        ]
        if part_uris
        else []
    )
    buckets = dict(map_tiles(combine_tile_sums, bucket_tasks, "buckets"))
    bucket_uris = [buckets[i] for i in sorted(buckets) if buckets[i]]

    schema = schema.with_metadata({**metadata, **aoi_source.provenance()})
    rows = _write_result(bucket_uris, result_uri, schema)
    _remove(parts_uri)
    summary = {
        "aois": len(aoi_bounds),
        "tiles": len(tiles_with_aois),
        "rows": rows,
        "result_uri": result_uri,
    }
    print(f"{aoi_source.aoi_type} zonal stats: {rows:,} rows -> {result_uri}")
    return summary


def _tiles_with_aois(tiles: List[Tile], aoi_bounds: pd.DataFrame) -> List[Tile]:
    """Select the tiles that at least one AOI's bounding box intersects."""
    if aoi_bounds.empty:
        return []
    tree = shapely.STRtree(
        shapely.box(*aoi_bounds[["xmin", "ymin", "xmax", "ymax"]].to_numpy().T)
    )
    tile_boxes = shapely.box(*np.array([bounds for _, _, bounds in tiles]).T)
    hit = np.unique(tree.query(tile_boxes, predicate="intersects")[0])
    return [tiles[i] for i in hit]


def _buckets(aoi_ids: np.ndarray) -> List[np.ndarray]:
    """Split sorted aoi_ids into consecutive groups of AOIS_PER_BUCKET."""
    return np.array_split(aoi_ids, max(1, int(np.ceil(len(aoi_ids) / AOIS_PER_BUCKET))))


def _write_result(bucket_uris: List[str], result_uri: str, schema: pa.Schema) -> int:
    """Append the bucket files, in order, into the result. Returns its rows."""
    rows = 0
    with fsspec.open(result_uri, "wb", auto_mkdir=True) as f, pq.ParquetWriter(
        f, schema, write_statistics=True
    ) as writer:
        if not bucket_uris:
            writer.write_table(schema.empty_table())
        for uri in bucket_uris:
            with fsspec.open(uri, "rb") as bucket:
                table = pq.read_table(bucket).cast(schema)
            writer.write_table(table, row_group_size=ROW_GROUP_SIZE)
            rows += table.num_rows
    return rows


def _remove(uri: str) -> None:
    filesystem, path = fsspec.core.url_to_fs(uri)
    if filesystem.exists(path):
        filesystem.rm(path, recursive=True)
