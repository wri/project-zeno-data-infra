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

The results don't fit in client memory, so a Dask dataframe groupby adds up each
AOI's partial sums across the parts on the cluster, partitioned so finish sees
whole groups; the client appends the partitions, in aoi_id order, into the
result.
"""

from dataclasses import dataclass
from functools import partial
from typing import Callable, Collection, Dict, List, Optional, Sequence, Tuple

import dask.dataframe as dd
import fsspec
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry.base import BaseGeometry

from pipelines.prefect_flows.tile_stages import Bounds, _dask_client, map_tiles
from pipelines.zonal_stats.aoi_sources import AOISource

# (row index range, column index range, bounds) of one tile of the grid
Tile = Tuple[slice, slice, Bounds]
# (row index range, column index range) -> the tile's candidate pixels as
# pixels: pixel-center x and y, the columns sums are grouped by and the column
# that is summed
TilePixels = Callable[[slice, slice], pd.DataFrame]

AOIS_PER_PARTITION = 20_000
ROW_GROUP_SIZE = 100_000


@dataclass(frozen=True)
class TileTask:
    """Describe the work of one tile's Dask task (compute_tile_sums)."""

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
    aoi_ids: List[str]
    """The AOIs whose bounding boxes intersect the tile, the only ones read."""
    read_pixels: TilePixels
    """The dataset's reader of the tile's candidate pixels."""
    by: List[str]
    """Names of the pixel columns the sums are grouped by."""
    sum_column: str
    """Name of the pixel column that is summed."""
    part_uri: str
    """File the tile's partial sums are written to."""


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


def compute_tile_sums(tile: TileTask) -> Optional[str]:
    """Write one tile's partial sums per AOI (steps 1-4 above) to a part file,
    as a Dask task. Returns the part's URI, or None if nothing in the tile
    is covered."""
    aois = tile.aoi_source.read(tile.bounds, tile.aoi_ids)
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


def compute_zonal_stats(
    aoi_source: AOISource,
    tiles: List[Tile],
    read_pixels: TilePixels,
    by: List[str],
    sum_column: str,
    finish: Callable[[pd.DataFrame], pd.DataFrame],
    schema: pa.Schema,
    result_uri: str,
    metadata: Dict[str, str],
    exclude_ids: Collection[str] = (),
    group_key: Optional[Callable[[pd.Series], pd.Series]] = None,
) -> Dict:
    """Compute zonal statistics of every AOI in aoi_source over the tiles and
    write them to result_uri. Returns a summary of the run. exclude_ids are
    AOIs not computed from pixels (e.g. admin units finish sums from their
    children); group_key maps aoi_ids to keys whose AOIs finish always sees
    together (e.g. admin units' country), which must sort like the aoi_ids."""
    aoi_bounds = aoi_source.read_bounds()
    aoi_bounds = aoi_bounds[~aoi_bounds["aoi_id"].isin(exclude_ids)]
    tiles_with_aois = _select_tiles_with_aois(tiles, aoi_bounds)
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
            aoi_ids=aoi_ids,
            read_pixels=read_pixels,
            by=by,
            sum_column=sum_column,
            part_uri=f"{parts_uri}/tiles/{ys.start}_{xs.start}.parquet",
        )
        for (ys, xs, bounds), aoi_ids in tiles_with_aois
    ]
    part_uris = [
        uri for uri in map_tiles(compute_tile_sums, tile_tasks, "tiles") if uri
    ]

    combined_uris = (
        _combine_tile_sums(
            part_uris,
            len(aoi_bounds),
            by,
            sum_column,
            finish,
            group_key,
            schema,
            f"{parts_uri}/combined",
        )
        if part_uris
        else []
    )

    schema = schema.with_metadata({**metadata, **aoi_source.provenance})
    rows = _write_result(combined_uris, result_uri, schema)
    _remove(parts_uri)
    summary = {
        "aois": len(aoi_bounds),
        "tiles": len(tiles_with_aois),
        "rows": rows,
        "result_uri": result_uri,
    }
    print(f"{aoi_source.aoi_type} zonal stats: {rows:,} rows -> {result_uri}")
    return summary


def _select_tiles_with_aois(
    tiles: List[Tile], aoi_bounds: pd.DataFrame
) -> List[Tuple[Tile, List[str]]]:
    """Select the tiles that at least one AOI's bounding box intersects, each
    with the ids of those AOIs."""
    if aoi_bounds.empty or not tiles:
        return []
    tree = shapely.STRtree(
        shapely.box(*aoi_bounds[["xmin", "ymin", "xmax", "ymax"]].to_numpy().T)
    )
    tile_boxes = shapely.box(*np.array([bounds for _, _, bounds in tiles]).T)
    pairs = pd.DataFrame(
        dict(zip(["tile", "aoi"], tree.query(tile_boxes, predicate="intersects")))
    )
    pairs["aoi_id"] = aoi_bounds["aoi_id"].to_numpy()[pairs["aoi"]]
    return [
        (tiles[tile], sorted(aoi_ids.unique()))
        for tile, aoi_ids in pairs.groupby("tile")["aoi_id"]
    ]


def _combine_tile_sums(
    part_uris: List[str],
    n_aois: int,
    by: List[str],
    sum_column: str,
    finish: Callable[[pd.DataFrame], pd.DataFrame],
    group_key: Optional[Callable[[pd.Series], pd.Series]],
    schema: pa.Schema,
    combined_uri: str,
) -> List[str]:
    """Add up each AOI's partial sums across the parts with a Dask dataframe
    groupby, finish them a partition at a time, and write each partition to a
    file under combined_uri. Returns the files, in aoi_id order."""
    with _dask_client():
        sums = (
            dd.read_parquet(part_uris)
            .groupby(["aoi_id", *by])[sum_column]
            .sum(split_out=True)
            .reset_index()
        )
        # Indexing on the group key keeps each group in one partition, and
        # orders the partitions by key, hence by aoi_id.
        key = sums["aoi_id"]
        if group_key is not None:
            key = key.map_partitions(group_key, meta=("aoi_id", "object"))
        sums = sums.assign(_key=key).set_index(
            "_key", npartitions=max(1, -(-n_aois // AOIS_PER_PARTITION))
        )
        finish_partition = partial(_finish_partition, finish=finish)
        finished = sums.map_partitions(
            finish_partition, meta=finish_partition(sums._meta)
        )
        finished.to_parquet(
            combined_uri,
            schema=schema,
            write_index=False,
            name_function=_partition_file_name,
        )
    filesystem, path = fsspec.core.url_to_fs(combined_uri)
    return [filesystem.unstrip_protocol(f) for f in sorted(filesystem.ls(path))]


def _finish_partition(sums: pd.DataFrame, finish) -> pd.DataFrame:
    return finish(sums.reset_index(drop=True))


def _partition_file_name(i: int) -> str:
    return f"{i:06d}.parquet"


def _write_result(combined_uris: List[str], result_uri: str, schema: pa.Schema) -> int:
    """Append the combined files, in order, into the result. Returns its rows."""
    rows = 0
    with fsspec.open(result_uri, "wb", auto_mkdir=True) as f, pq.ParquetWriter(
        f, schema, write_statistics=True
    ) as writer:
        if not combined_uris:
            writer.write_table(schema.empty_table())
        for uri in combined_uris:
            with fsspec.open(uri, "rb") as f:
                table = pq.read_table(f).cast(schema)
            writer.write_table(table, row_group_size=ROW_GROUP_SIZE)
            rows += table.num_rows
    return rows


def _remove(uri: str) -> None:
    filesystem, path = fsspec.core.url_to_fs(uri)
    if filesystem.exists(path):
        filesystem.rm(path, recursive=True)
