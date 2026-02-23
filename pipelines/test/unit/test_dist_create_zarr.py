from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr

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


@patch("pipelines.prefect_flows.common_stages.s3_uri_exists")
@patch("pipelines.prefect_flows.common_stages.xr.open_dataset")
def test_create_zarr_new_file(mock_open_dataset, mock_s3_exists, mock_dataset):
    """Test creating a new zarr file when it doesn't exist."""
    mock_s3_exists.return_value = False
    mock_open_dataset.return_value = mock_dataset

    version = "v20250102"
    cog_uri = "s3://gfw-data-lake/umd_glad_dist_alerts/v20250102/raster/epsg-4326/cog/default.tif"
    expected_uri = (
        "s3://lcl-analytics/zarr/dist-alerts/v20250102/umd_glad_dist_alerts.zarr"
    )

    with patch.object(xr.Dataset, "to_zarr") as mock_to_zarr:
        # Test via the disturbance convenience wrapper (includes decode_alert_data)
        result = create_zarr(version, overwrite=False)

        assert result == expected_uri

        mock_s3_exists.assert_called_once_with(
            f"{expected_uri}/zarr.json",
        )

        mock_open_dataset.assert_called_once_with(cog_uri, chunks="auto")

        mock_to_zarr.assert_called_once_with(expected_uri, mode="w")


@patch("pipelines.prefect_flows.common_stages.s3_uri_exists")
@patch("pipelines.prefect_flows.common_stages.xr.open_dataset")
def test_create_zarr_generic_no_transform(
    mock_open_dataset, mock_s3_exists, mock_dataset
):
    """Test the generic create_zarr without a transform."""
    from pipelines.prefect_flows.common_stages import create_zarr as generic_create_zarr

    mock_s3_exists.return_value = False
    mock_open_dataset.return_value = mock_dataset

    cog_uri = "s3://bucket/source.tif"
    zarr_uri = "s3://bucket/output.zarr"

    with patch.object(xr.DataArray, "to_zarr") as mock_to_zarr:
        result = generic_create_zarr(cog_uri, zarr_uri)

        assert result == zarr_uri
        mock_open_dataset.assert_called_once_with(cog_uri, chunks="auto")
        mock_to_zarr.assert_called_once_with(zarr_uri, mode="w")


@patch("pipelines.prefect_flows.common_stages.s3_uri_exists")
@patch("pipelines.prefect_flows.common_stages._load_tile_uris")
@patch("pipelines.prefect_flows.common_stages.xr.open_mfdataset")
def test_create_zarr_tiled_geotiff(
    mock_open_mfdataset, mock_load_tiles, mock_s3_exists, mock_dataset
):
    """Test create_zarr with a tiled GeoTIFF source (tiles.geojson)."""
    from pipelines.prefect_flows.common_stages import create_zarr as generic_create_zarr

    mock_s3_exists.return_value = False
    tile_uris = ["s3://bucket/tile_0.tif", "s3://bucket/tile_1.tif"]
    mock_load_tiles.return_value = tile_uris
    mock_open_mfdataset.return_value = mock_dataset

    source_uri = "s3://bucket/tiles.geojson"
    zarr_uri = "s3://bucket/output.zarr"
    chunks = {"x": 10000, "y": 10000}

    with patch.object(xr.DataArray, "to_zarr") as mock_to_zarr:
        result = generic_create_zarr(source_uri, zarr_uri)

        assert result == zarr_uri
        mock_load_tiles.assert_called_once_with(source_uri)
        mock_open_mfdataset.assert_called_once_with(
            tile_uris, parallel=True, chunks=chunks
        )
        mock_to_zarr.assert_called_once_with(zarr_uri, mode="w")
