from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr

from app.analysis.common.analysis import (
    clip_zarr_to_geojson,
    read_zarr_clipped_to_geojson,
    resample_to_grid,
    to_land_filter_mask,
)

GEOJSON = {
    "type": "Polygon",
    "coordinates": [
        [
            [-72.0, -10.0],
            [-71.0, -10.0],
            [-71.0, -9.0],
            [-72.0, -9.0],
            [-72.0, -10.0],
        ]
    ],
}


class TestClipZarrToGeojson:
    def test_dimensionless_dataset_raises_key_error(self):
        """Reproduces the exact error from carbon_flux_analytics_processing_failure:
        KeyError: "'x' is not a valid dimension or coordinate for Dataset with
        dimensions FrozenMappingWarningOnValuesAccess({})"

        This happens when xr.open_zarr() returns a Dataset with no dimensions,
        e.g. when the zarr URI points to a group container rather than an array.
        """
        empty_ds = xr.Dataset()

        with pytest.raises(KeyError, match="'x' is not a valid dimension"):
            clip_zarr_to_geojson(empty_ds, GEOJSON)


class TestReadZarrClippedToGeojson:
    @patch("app.analysis.common.analysis.read_zarr")
    def test_dimensionless_zarr_raises_value_error(self, mock_read_zarr):
        """Verifies that the guard in read_zarr_clipped_to_geojson surfaces a
        descriptive ValueError instead of letting the KeyError propagate from
        clip_zarr_to_geojson."""
        mock_read_zarr.return_value = xr.Dataset()

        with pytest.raises(ValueError, match="opened with no dimensions"):
            read_zarr_clipped_to_geojson("s3://gfw-data-lake/any.zarr/", GEOJSON)


def _pixel_centers(n_pixels, resolution):
    """Pixel centers starting at a whole degree, like the real tiled grids."""
    return np.arange(n_pixels) * resolution + resolution / 2


class TestResampleToGrid:
    def test_upsamples_30m_to_10m(self):
        """Each 10m pixel takes the value of the 30m pixel its center falls in, and
        10m pixels outside the 30m extent get 0. A tolerance like 1e-5 would match
        no 10m pixels at all, since 10m and 30m pixel centers never coincide."""
        x30 = _pixel_centers(2, 0.00025)
        layer = xr.DataArray(
            np.array([[1, 0], [0, 1]], dtype=np.uint8),
            dims=("y", "x"),
            coords={"y": -x30, "x": x30},
        )
        # 10m grid covering the two 30m pixels (5 10m pixels), plus 2 beyond.
        x10 = _pixel_centers(7, 0.0001)
        target = xr.DataArray(
            np.zeros((7, 7), dtype=np.uint8),
            dims=("y", "x"),
            coords={"y": -x10, "x": x10},
        )

        values = resample_to_grid(layer, target).values

        # 10m pixel index 2 is centered exactly on the 30m pixel edge.
        first, second, outside = [0, 1], [3, 4], [5, 6]
        assert values[np.ix_(first, first)].tolist() == [[1, 1]] * 2
        assert values[np.ix_(first, second)].tolist() == [[0, 0]] * 2
        assert values[np.ix_(second, second)].tolist() == [[1, 1]] * 2
        assert (values[outside, :] == 0).all()
        assert (values[:, outside] == 0).all()


def test_to_land_filter_mask_marks_classes_2_to_11_as_natural_lands():
    classes = xr.DataArray(np.array([0, 1, 2, 11, 12, 21], dtype=np.uint8))

    np.testing.assert_array_equal(
        to_land_filter_mask(classes, "natural_lands").values, [0, 0, 1, 1, 0, 0]
    )
