import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

from pipelines.dist.check_for_new_alerts import (
    get_latest_version,
    s3_object_exists,
)


@patch("pipelines.dist.check_for_new_alerts.requests.get")
def test_get_latest_version_success(mock_get):
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "status": "success",
        "data": {"version": "v20250501"},
    }
    mock_get.return_value = mock_response

    version = get_latest_version("umd_glad_dist_alerts")
    assert version == "v20250501"
    mock_get.assert_called_once_with(
        "https://data-api.globalforestwatch.org/dataset/umd_glad_dist_alerts/latest"
    )


@patch("pipelines.dist.check_for_new_alerts.requests.get")
def test_get_latest_version_failure_status(mock_get):
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"status": "error", "data": {}}
    mock_get.return_value = mock_response

    with pytest.raises(ValueError):
        get_latest_version("invalid_dataset")


@patch("pipelines.dist.check_for_new_alerts.boto3.client")
def test_s3_object_exists_true(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.head_object.return_value = {}

    assert s3_object_exists("my-bucket", "my/key.zarr") is True
    mock_s3.head_object.assert_called_once_with(Bucket="my-bucket", Key="my/key.zarr")


@patch("pipelines.dist.check_for_new_alerts.boto3.client")
def test_s3_object_exists_false(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    error_response = {"Error": {"Code": "404"}}
    mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")

    assert s3_object_exists("my-bucket", "missing/key.zarr") is False


@patch("pipelines.dist.check_for_new_alerts.boto3.client")
def test_s3_object_exists_other_error(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    error_response = {"Error": {"Code": "403"}}
    mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")

    with pytest.raises(ClientError):
        s3_object_exists("my-bucket", "forbidden/key.zarr")


@patch("pipelines.dist.check_for_new_alerts.boto3.client")
def test_s3_object_exists_other_error(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    error_response = {"Error": {"Code": "403"}}
    mock_s3.head_object.side_effect = ClientError(error_response, "HeadObject")

    with pytest.raises(ClientError):
        s3_object_exists("my-bucket", "forbidden/key.zarr")
