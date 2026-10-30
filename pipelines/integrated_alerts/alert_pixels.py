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

from pipelines.globals import sbtn_natural_lands_zarr_uri
from pipelines.integrated_alerts.stages import check_square_pixels
from pipelines.prefect_flows.tile_stages import (
    sample_nearest,
    written_chunks,
)

ALERT_EPOCH = date(2014, 12, 31)  # alert_date counts days since this date
CONFIDENCES = (2, 3, 4)  # low, high, highest

# (rows, cols) of one alerts tile on the global grid
Tile = Tuple[slice, slice]


def open_zarrs(
    alerts_zarr_uri: str, natural_lands_zarr_uri: str = sbtn_natural_lands_zarr_uri
) -> Dict:
    """The alerts (alert_date, confidence) and natural lands zarrs, without the
    band dimension, for reading tiles inside a Dask task. They're opened without
    dask (chunks=None), so indexing reads only the zarr chunks it touches with no
    nested dask graph, and with cache=False, so datasets kept on a worker don't
    hold array data."""
    options = {"engine": "zarr", "chunks": None, "cache": False}
    alerts = xr.open_dataset(alerts_zarr_uri, **options).squeeze("band", drop=True)
    natural_lands = xr.open_dataset(natural_lands_zarr_uri, **options)[
        "band_data"
    ].squeeze("band", drop=True)
    check_square_pixels(natural_lands, "natural lands")
    return {"alerts": alerts, "natural_lands": natural_lands}


def tile_alert_pixels(
    zarrs: Dict,
    ys: slice,
    xs: slice,
    day_lo: int,
    day_hi: int,
    confidences: Iterable[int] = CONFIDENCES,
) -> pd.DataFrame:
    """The alert pixels in the tile at rows ys and columns xs of the alerts grid
    with alert_date in [day_lo, day_hi] and confidence in confidences: global
    row and column, alert_date, confidence and natural_lands_class. Each 10 m
    pixel takes the class of the 30 m pixel its center is nearest, within half a
    30 m pixel, else 0, as resample_to_alerts_grid does."""
    tile = zarrs["alerts"][["alert_date", "confidence"]].isel(y=ys, x=xs).load()
    alert_date, confidence = tile["alert_date"].values, tile["confidence"].values
    rows, cols = np.nonzero(
        (alert_date >= day_lo)
        & (alert_date <= day_hi)
        & np.isin(confidence, list(confidences))
    )
    natural_lands = zarrs["natural_lands"]
    natural_lands_resolution = abs(float(natural_lands.x[1] - natural_lands.x[0]))
    natural_lands_class = sample_nearest(
        natural_lands,
        tile.x.values[cols],
        tile.y.values[rows],
        tolerance=natural_lands_resolution / 2,
    )
    return pd.DataFrame(
        {
            "row": (ys.start + rows).astype(np.int32),
            "col": (xs.start + cols).astype(np.int32),
            "alert_date": alert_date[rows, cols].astype(np.int16),
            "confidence": confidence[rows, cols].astype(np.int8),
            "natural_lands_class": natural_lands_class.astype(np.uint8),
        }
    )


def alert_tiles(alerts_zarr_uri: str, bbox: Optional[Polygon] = None) -> List[Tile]:
    """The alerts zarr's chunks that hold data, as (rows, cols) tiles in row
    then column order. With a bbox, tiles are narrowed to the pixels
    clip_ds_to_bbox would keep, and tiles outside it are dropped."""
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
