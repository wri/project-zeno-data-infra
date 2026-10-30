"""Reading the integrated alerts zarr tile by tile, for the pipelines that work
from alert pixels (the alert points table, vector zonal stats): the tiles that
hold data, and the alert pixels of a tile with the SBTN natural lands class of
each."""

from datetime import date
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import Polygon

from pipelines.integrated_alerts.stages import check_square_pixels
from pipelines.prefect_flows.tile_stages import (
    sample_nearest,
    written_chunks,
)

ALERT_EPOCH = date(2014, 12, 31)  # alert_date counts days since this date
CONFIDENCES = (2, 3, 4)  # low, high, highest

# (rows, cols) of one alerts tile on the global grid
Tile = Tuple[slice, slice]


def open_zarrs(alerts_zarr_uri: str, layer_uris: Dict[str, str]) -> Dict:
    """Open the alerts zarr and the named layers for reading tiles inside a Dask
    task: without dask (chunks=None), so indexing reads only the chunks it
    touches, and with cache=False, so datasets kept on a worker hold no array
    data. Layers are keyed by the column name they're read into."""
    options = {"engine": "zarr", "chunks": None, "cache": False}
    zarrs = {
        "alerts": xr.open_dataset(alerts_zarr_uri, **options).squeeze("band", drop=True)
    }
    for name, uri in layer_uris.items():
        # requester pays for layers in other accounts' buckets, e.g. the data lake
        storage = {"requester_pays": True} if uri.startswith("s3://") else None
        layer = xr.open_dataset(uri, storage_options=storage, **options)["band_data"]
        layer = layer.squeeze("band", drop=True) if "band" in layer.dims else layer
        check_square_pixels(layer, name)
        zarrs[name] = layer
    return zarrs


def read_tile_alert_pixels(
    zarrs: Dict,
    ys: slice,
    xs: slice,
    day_lo: int,
    day_hi: int,
    confidences: Iterable[int] = CONFIDENCES,
) -> pd.DataFrame:
    """Read the alert pixels in a tile (rows ys, columns xs of the alerts grid)
    with alert_date in [day_lo, day_hi] and confidence in confidences, with
    their position and every layer in zarrs sampled at their centers. A pixel
    takes the value of the layer pixel nearest its center, within half a layer
    pixel, else 0, as resample_to_alerts_grid does."""
    tile = zarrs["alerts"][["alert_date", "confidence"]].isel(y=ys, x=xs).load()
    alert_date, confidence = tile["alert_date"].values, tile["confidence"].values
    rows, cols = np.nonzero(
        (alert_date >= day_lo)
        & (alert_date <= day_hi)
        & np.isin(confidence, list(confidences))
    )
    x, y = tile.x.values[cols], tile.y.values[rows]
    layers = {
        name: sample_nearest(
            layer, x, y, tolerance=abs(float(layer.x[1] - layer.x[0])) / 2
        )
        for name, layer in zarrs.items()
        if name != "alerts"
    }
    return pd.DataFrame(
        {
            "row": (ys.start + rows).astype(np.int32),
            "col": (xs.start + cols).astype(np.int32),
            "x": x,
            "y": y,
            "alert_date": alert_date[rows, cols].astype(np.int16),
            "confidence": confidence[rows, cols].astype(np.int8),
            **layers,
        }
    )


def alert_tiles(alerts_zarr_uri: str, bbox: Optional[Polygon] = None) -> List[Tile]:
    """List the alerts zarr's chunks that hold data as tiles, narrowed to bbox
    if one is given."""
    # dask-backed, so its chunk grid defines the tiles
    grid = xr.open_zarr(alerts_zarr_uri)["alert_date"].squeeze("band", drop=True)
    chunk_rows, chunk_cols = grid.data.chunksize
    rows, cols = slice(0, grid.sizes["y"]), slice(0, grid.sizes["x"])
    if bbox is not None:
        min_x, min_y, max_x, max_y = bbox.bounds
        # the positions clip_ds_to_bbox's label slices select (y descending)
        rows = grid.get_index("y").slice_indexer(max_y, min_y)
        cols = grid.get_index("x").slice_indexer(min_x, max_x)

    tiles = []
    for i, j in sorted(written_chunks(alerts_zarr_uri, "alert_date")):
        ys = slice(
            max(i * chunk_rows, rows.start), min((i + 1) * chunk_rows, rows.stop)
        )
        xs = slice(
            max(j * chunk_cols, cols.start), min((j + 1) * chunk_cols, cols.stop)
        )
        if ys.start < ys.stop and xs.start < xs.stop:
            tiles.append((ys, xs))
    return tiles
