import xarray as xr
import numpy as np

from pipelines.dist.validate_tiles import _check_percent_diff

def test_check_percentage_diff():
    """
    Test percentage diff on two arrays, where one if all ones, and the other is half ones
    """
    dims = ('y', 'x')
    coords = {
        'y': np.arange(0, 10),
        'x': np.arange(0, 10)
    }

    data1 = np.ones((10, 10))
    da1 = xr.DataArray(data1, dims=dims, coords=coords, name='da1')

    data2 = np.ones((10, 10))
    data2[:5, :] = np.nan
    da2 = xr.DataArray(data2, dims=dims, coords=coords, name='da2')

    assert _check_percent_diff(da1, da2, 70)
    assert not _check_percent_diff(da1, da2, 60)
