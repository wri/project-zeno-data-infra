from unittest.mock import patch

import pytest
import xarray as xr

from app.analysis.common.analysis import (
    clip_zarr_to_geojson,
    read_zarr_clipped_to_geojson,
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
