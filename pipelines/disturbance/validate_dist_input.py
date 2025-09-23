import numpy as np
import xarray as xr

DIST_COUNT_THRESHOLD = 5


def validates_dist_input(curr_cog_uri: str, prev_cog_uri: str) -> None:
    """
    Validate DIST tiles on Data API to confirm there's no issues with the input data. Directly
    pulls global stats on the current and previous tiles, and validate there hasn't been an
    unexpectedly large change.
    """
    curr_dist_alerts = _read_from_cog(curr_cog_uri)
    prev_dist_alerts = _read_from_cog(prev_cog_uri)

    assert _check_percent_diff(curr_dist_alerts, prev_dist_alerts, DIST_COUNT_THRESHOLD)


def _check_percent_diff(
    da1: xr.DataArray, da2: xr.DataArray, diff_threshold: float
) -> bool:
    stat1 = da1.count()
    stat2 = da2.count()

    percent_diff = (abs(stat1 - stat2) / ((stat1 + stat2) / 2)).item() * 100
    return percent_diff < diff_threshold


def _read_from_cog(cog_uri: str):
    """
    Read COG to an xarray. Using chunking the aligns with COG internal tiling for faster stats, since
    this read isn't being used for converting to zarr.
    """
    dist_alerts = (
        xr.open_dataset(cog_uri, chunks={"x": 8192, "y": 8192})
        .astype(np.int16)
        .band_data
    )

    return dist_alerts
