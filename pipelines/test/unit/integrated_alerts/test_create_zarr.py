import io
import json
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from pipelines.integrated_alerts.create_zarr import create_zarr, decode_alert_data


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


@pytest.fixture
def mock_tiles_geojson_body():
    geojson = {
        "features": [
            {
                "properties": {
                    "name": "gfw-data-lake/gfw_integrated_dist_alerts/v20250102/"
                    "raster/epsg-4326/10/100000/date_conf/geotiff/tile_0_0.tif"
                }
            }
        ]
    }
    return io.BytesIO(json.dumps(geojson).encode())


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


def _create_zarr_new_file(
    version, mock_open_mfdataset, mock_boto3_client, mock_s3_exists, mock_dataset,
    mock_tiles_geojson_body,
):
    mock_s3_exists.return_value = False
    mock_open_mfdataset.return_value = mock_dataset

    mock_s3_client = MagicMock()
    mock_s3_client.get_object.return_value = {"Body": mock_tiles_geojson_body}
    mock_boto3_client.return_value = mock_s3_client

    with patch.object(xr.Dataset, "to_zarr") as mock_to_zarr:
        result = create_zarr(version, overwrite=False)
    return result, mock_to_zarr


@pytest.mark.parametrize(
    "version",
    [
        "v20250102",  # Thu, outside Jan 2025's first-Sunday week
        "v20250105",  # first Sunday of Jan 2025
    ],
)
@patch("pipelines.integrated_alerts.create_zarr.s3_uri_exists")
@patch("pipelines.integrated_alerts.create_zarr.boto3.client")
@patch("pipelines.integrated_alerts.create_zarr.xr.open_mfdataset")
def test_create_zarr_builds_fresh_zarr_reports_freshly_created(
    mock_open_mfdataset,
    mock_boto3_client,
    mock_s3_exists,
    version,
    mock_dataset,
    mock_tiles_geojson_body,
):
    """Building a new zarr reports freshly_created=True regardless of
    first-Sunday-week status -- create_zarr() no longer decides whether
    extra_processing_tasks() should run; that's the caller's job (see
    is_first_sunday_week usage in integrated_alerts_flow.py), combining
    this flag with its own is_first_sunday_week(version) check (defined in
    extra_processing.py). create_zarr() itself never touches the
    monitoring-bucket copy, GEE registration, or GCS transfer -- that's all
    in extra_processing.py."""
    result, mock_to_zarr = _create_zarr_new_file(
        version,
        mock_open_mfdataset,
        mock_boto3_client,
        mock_s3_exists,
        mock_dataset,
        mock_tiles_geojson_body,
    )

    expected_zarr_uri = (
        f"s3://lcl-analytics/zarr/gfw_integrated_dist_alerts/{version}/date_conf.zarr"
    )

    assert result == (expected_zarr_uri, True)
    mock_to_zarr.assert_called_once_with(expected_zarr_uri, mode="w")


@patch("pipelines.integrated_alerts.create_zarr.s3_uri_exists")
def test_create_zarr_skips_build_when_already_exists(mock_s3_exists):
    """No new zarr is built when it already exists, and freshly_created is
    always False on that path."""
    mock_s3_exists.return_value = True

    result = create_zarr("v20250105", overwrite=False)

    assert result == (
        "s3://lcl-analytics/zarr/gfw_integrated_dist_alerts/v20250105/date_conf.zarr",
        False,
    )
