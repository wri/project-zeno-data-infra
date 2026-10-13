import numpy as np
import xarray as xr

from pipelines.integrated_alerts.stages import resample_to_alerts_grid

# Real grid spacings: SBTN natural lands is 30m, integrated alerts is 10m.
NATURAL_LANDS_RESOLUTION = 0.00025
ALERTS_RESOLUTION = 0.0001


def _grid(n_pixels, resolution):
    """Pixel centers, starting at a whole degree like the real tiled grids."""
    return np.arange(n_pixels) * resolution + resolution / 2


def test_resample_to_alerts_grid_upsamples_30m_to_10m():
    """Each 10m pixel takes the value of the 30m pixel its center falls in, and 10m
    pixels outside the 30m extent get 0. A tolerance like 1e-5 would match no 10m
    pixels at all, since 10m and 30m pixel centers never coincide."""
    x30 = _grid(2, NATURAL_LANDS_RESOLUTION)
    layer = xr.DataArray(
        np.array([[1, 0], [0, 1]], dtype=np.uint8),
        dims=("y", "x"),
        coords={"y": -x30, "x": x30},
    )
    # 10m grid covering the two 30m pixels (5 10m pixels), plus 2 pixels beyond.
    x10 = _grid(7, ALERTS_RESOLUTION)
    alerts = xr.DataArray(
        np.zeros((7, 7), dtype=np.uint8),
        dims=("y", "x"),
        coords={"y": -x10, "x": x10},
    )

    resampled = resample_to_alerts_grid(layer, alerts)

    # 10m pixel centers at 0.00005, 0.00015, [0.00025 is on the 30m edge],
    # 0.00035, 0.00045, then 0.00055, 0.00065 outside the 30m extent.
    first_30m_pixel, second_30m_pixel, outside = [0, 1], [3, 4], [5, 6]
    values = resampled.values
    assert values[np.ix_(first_30m_pixel, first_30m_pixel)].tolist() == [[1, 1]] * 2
    assert values[np.ix_(first_30m_pixel, second_30m_pixel)].tolist() == [[0, 0]] * 2
    assert values[np.ix_(second_30m_pixel, second_30m_pixel)].tolist() == [[1, 1]] * 2
    assert (values[outside, :] == 0).all()
    assert (values[:, outside] == 0).all()
    assert resampled.dtype == np.uint8
