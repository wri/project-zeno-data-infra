"""Integrated alert pixels of one tile of the alerts zarr, with the SBTN natural
lands class of each."""

from typing import Dict, Iterable

import numpy as np
import pandas as pd
import xarray as xr

from pipelines.globals import sbtn_natural_lands_zarr_uri
from pipelines.integrated_alerts.stages import check_square_pixels
from pipelines.prefect_flows.tile_stages import sample_nearest

CONFIDENCES = (2, 3, 4)  # low, high, highest


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
