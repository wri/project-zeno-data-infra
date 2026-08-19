from unittest.mock import patch

import pytest

from pipelines.integrated_alerts.extra_processing import (
    extra_processing_tasks,
    is_first_sunday_week,
)


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
    assert is_first_sunday_week(version) == expected


@patch("pipelines.integrated_alerts.extra_processing.transfer_s3_to_gcs")
@patch("pipelines.integrated_alerts.extra_processing.register_gee_asset")
@patch("pipelines.integrated_alerts.extra_processing.copy_s3_directory")
def test_extra_processing_tasks_copies_and_registers_gee_assets(
    mock_copy_s3_directory, mock_register_gee_asset, mock_transfer_s3_to_gcs
):
    """Copies the zarr to the monitoring bucket, registers the GEE asset in
    forma-250, transfers the COG to GCS, and registers that transferred
    copy as a GEE asset in landandcarbon."""
    version = "v20250105"
    zarr_uri = "s3://lcl-analytics/zarr/gfw_integrated_dist_alerts/v20250105/date_conf.zarr"

    extra_processing_tasks(version, zarr_uri)

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
