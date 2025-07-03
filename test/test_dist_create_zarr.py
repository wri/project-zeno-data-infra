import pytest
import numpy as np
import xarray as xr
from unittest.mock import patch

from pipelines.disturbance.create_zarr import create_zarr, decode_alert_data


@pytest.fixture
def mock_dataset():
    """Create a mock xarray dataset with band_data variable."""
    x_coords = np.linspace(-180, 180, 100)
    y_coords = np.linspace(-90, 90, 100)

    data = np.random.randint(10000, 30000, size=(100, 100))

    dataset = xr.Dataset(
        {
            "band_data": xr.DataArray(
                data, dims=["y", "x"], coords={"x": x_coords, "y": y_coords}
            )
        }
    )

    return dataset


def test_decode_alert_data():
    test_data = np.array(
        [
            [23456, 15789],  # conf=2,date=3456 | conf=1,date=5789
            [30001, 40000],  # conf=3,date=1    | conf=4,date=0
        ]
    )

    test_array = xr.DataArray(
        test_data, dims=["y", "x"], coords={"x": [0, 1], "y": [0, 1]}
    )

    result = decode_alert_data(test_array)

    assert "confidence" in result.data_vars
    assert "alert_date" in result.data_vars
    assert result.confidence.dtype == np.uint8
    assert result.alert_date.dtype == np.uint16

    assert result.confidence.values[0, 0] == 2
    assert result.alert_date.values[0, 0] == 3456
    assert result.confidence.values[0, 1] == 1
    assert result.alert_date.values[0, 1] == 5789
    assert result.confidence.values[1, 0] == 3
    assert result.alert_date.values[1, 0] == 1
    assert result.confidence.values[1, 1] == 4
    assert result.alert_date.values[1, 1] == 0

    assert list(result.x.values) == [0, 1]
    assert list(result.y.values) == [0, 1]


@patch("pipelines.disturbance.create_zarr.data_lake_bucket", "test-bucket")
@patch("pipelines.disturbance.create_zarr.s3_object_exists")
@patch("pipelines.disturbance.create_zarr.xr.open_dataset")
def test_create_zarr_new_file(mock_open_dataset, mock_s3_exists, mock_dataset):
    """Test creating a new zarr file when it doesn't exist."""
    mock_s3_exists.return_value = False
    mock_open_dataset.return_value = mock_dataset

    version = "v20250102"
    cog_uri = f"s3://test-bucket/umd_glad_dist_alerts/{version}/raster/epsg-4326/cog/default.tif"

    with patch.object(xr.Dataset, "to_zarr") as mock_to_zarr:

        result = create_zarr(version, overwrite=False)

        expected_uri = f"s3://test-bucket/umd_glad_dist_alerts/{version}/raster/epsg-4326/zarr/umd_glad_dist_alerts_{version}.zarr"
        assert result == expected_uri

        mock_s3_exists.assert_called_once_with(
            "test-bucket",
            f"umd_glad_dist_alerts/{version}/raster/epsg-4326/zarr/umd_glad_dist_alerts_{version}.zarr/zarr.json",
        )

        mock_open_dataset.assert_called_once_with(cog_uri, chunks="auto")

        mock_to_zarr.assert_called_once_with(expected_uri, mode="w")
