import io
import json
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from pipelines.integrated_alerts.create_zarr import (
    _is_first_sunday_week,
    create_zarr,
    decode_alert_data,
    first_sunday_processing,
)


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


@pytest.mark.parametrize(
    "version,expected",
    [
        ("v20250102", False),  # Thu, before Jan 2025's first Sunday (Jan 5)
        ("v20250105", True),  # first Sunday of Jan 2025
        ("v20250107", True),  # Tue, delayed version still in the first-Sunday week
        ("v20250112", False),  # second Sunday of Jan 2025
        ("v20260802", True),  # first Sunday of Aug 2026 falls on the 2nd
        ("v20240107", True),  # month starting on a Monday: first Sunday is the 7th
        ("v20240101", False),  # before that month's first Sunday
    ],
)
def test_is_first_sunday_week(version, expected):
    assert _is_first_sunday_week(version) == expected


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


@patch("pipelines.integrated_alerts.create_zarr.s3_uri_exists")
@patch("pipelines.integrated_alerts.create_zarr.boto3.client")
@patch("pipelines.integrated_alerts.create_zarr.xr.open_mfdataset")
def test_create_zarr_outside_first_sunday_week_reports_no_further_processing(
    mock_open_mfdataset,
    mock_boto3_client,
    mock_s3_exists,
    mock_dataset,
    mock_tiles_geojson_body,
):
    """A version outside the first-Sunday-of-month week still builds the
    zarr, but reports run_first_sunday_processing=False. create_zarr()
    itself never touches the monitoring-bucket copy, GEE registration, or
    GCS transfer anymore -- that's all in first_sunday_processing()."""
    version = "v20250102"  # Thu, before Jan 2025's first Sunday (Jan 5)

    result, mock_to_zarr = _create_zarr_new_file(
        version,
        mock_open_mfdataset,
        mock_boto3_client,
        mock_s3_exists,
        mock_dataset,
        mock_tiles_geojson_body,
    )

    expected_zarr_uri = (
        "s3://lcl-analytics/zarr/gfw_integrated_dist_alerts/v20250102/date_conf.zarr"
    )

    assert result == (expected_zarr_uri, False)
    mock_to_zarr.assert_called_once_with(expected_zarr_uri, mode="w")


@patch("pipelines.integrated_alerts.create_zarr.s3_uri_exists")
@patch("pipelines.integrated_alerts.create_zarr.boto3.client")
@patch("pipelines.integrated_alerts.create_zarr.xr.open_mfdataset")
def test_create_zarr_in_first_sunday_week_reports_further_processing_needed(
    mock_open_mfdataset,
    mock_boto3_client,
    mock_s3_exists,
    mock_dataset,
    mock_tiles_geojson_body,
):
    """A version in the first-Sunday-of-month week builds the zarr and
    reports run_first_sunday_processing=True, so the caller knows to run
    first_sunday_processing() -- create_zarr() does not run it itself."""
    version = "v20250105"  # first Sunday of Jan 2025

    result, mock_to_zarr = _create_zarr_new_file(
        version,
        mock_open_mfdataset,
        mock_boto3_client,
        mock_s3_exists,
        mock_dataset,
        mock_tiles_geojson_body,
    )

    expected_zarr_uri = (
        "s3://lcl-analytics/zarr/gfw_integrated_dist_alerts/v20250105/date_conf.zarr"
    )

    assert result == (expected_zarr_uri, True)
    mock_to_zarr.assert_called_once_with(expected_zarr_uri, mode="w")


@patch("pipelines.integrated_alerts.create_zarr.s3_uri_exists")
def test_create_zarr_skips_build_when_already_exists(mock_s3_exists):
    """No new zarr is built when it already exists, and
    run_first_sunday_processing is always False on that path -- the caller
    should assume post-processing already happened on the run that
    actually created this version's zarr."""
    mock_s3_exists.return_value = True

    result = create_zarr("v20250105", overwrite=False)

    assert result == (
        "s3://lcl-analytics/zarr/gfw_integrated_dist_alerts/v20250105/date_conf.zarr",
        False,
    )


@patch("pipelines.integrated_alerts.create_zarr.transfer_s3_to_gcs")
@patch("pipelines.integrated_alerts.create_zarr.register_gee_asset")
@patch("pipelines.integrated_alerts.create_zarr.copy_s3_directory")
def test_first_sunday_processing_copies_and_registers_gee_assets(
    mock_copy_s3_directory, mock_register_gee_asset, mock_transfer_s3_to_gcs
):
    """Copies the zarr to the monitoring bucket, registers the GEE asset in
    forma-250, transfers the COG to GCS, and registers that transferred
    copy as a GEE asset in landandcarbon."""
    version = "v20250105"
    zarr_uri = "s3://lcl-analytics/zarr/gfw_integrated_dist_alerts/v20250105/date_conf.zarr"

    first_sunday_processing(version, zarr_uri)

    expected_monitoring_uri = (
        "s3://gnw-monitoring-data/zarrs/intdist.date_conf.v20250105.zarr"
    )
    mock_copy_s3_directory.assert_called_once_with(zarr_uri, expected_monitoring_uri)

    assert mock_register_gee_asset.call_count == 2
    mock_register_gee_asset.assert_any_call(
        "gs://data-api-gee-assets/gfw_integrated_dist_alerts/intdist_tropics.tif",
        "gfw_integrated_dist_alerts/intdist_tropics",
        project="forma-250",
        force=True,
    )
    mock_register_gee_asset.assert_any_call(
        "gs://wri-lcl-integrated-alerts/intdist_tropics.tif",
        "integrated_dist_alerts/intdist_tropics",
        project="landandcarbon",
        force=True,
    )
    mock_transfer_s3_to_gcs.assert_called_once_with(
        "s3://gfw-data-lake/gfw_integrated_dist_alerts/v20250105/raster/epsg-4326/cog/",
        "gs://wri-lcl-integrated-alerts/",
        "s3-auth/gfw-data-lake-readonly",
        project="landandcarbon",
        include_prefixes=["intdist_tropics.tif"],
        timeout=7200,
    )
