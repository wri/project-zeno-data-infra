from unittest.mock import MagicMock, patch

import pytest

from pipelines.utils import InvalidS3UriError, copy_s3_directory, get_secret, parse_s3_uri


@patch("pipelines.utils.boto3.client")
def test_get_secret_returns_secret_string(mock_boto3_client):
    mock_client = MagicMock()
    mock_boto3_client.return_value = mock_client
    mock_client.get_secret_value.return_value = {"SecretString": "the-secret-value"}

    assert get_secret("gcs-auth/forma-250") == "the-secret-value"

    mock_boto3_client.assert_called_once_with("secretsmanager")
    mock_client.get_secret_value.assert_called_once_with(SecretId="gcs-auth/forma-250")


def test_parse_s3_uri():
    assert parse_s3_uri("s3://my-bucket/some/key.zarr") == (
        "my-bucket",
        "some/key.zarr",
    )


def test_parse_s3_uri_invalid():
    with pytest.raises(InvalidS3UriError):
        parse_s3_uri("not-an-s3-uri")


@patch("pipelines.utils.boto3.client")
def test_copy_s3_directory_copies_every_object_with_relative_key(mock_boto3_client):
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    mock_s3_client.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {"Key": "src-prefix/zarr.json"},
                {"Key": "src-prefix/confidence/0.0"},
            ]
        }
    ]

    copy_s3_directory(
        "s3://src-bucket/src-prefix", "s3://dst-bucket/dst-prefix"
    )

    mock_s3_client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="src-bucket", Prefix="src-prefix/", RequestPayer="requester"
    )
    mock_s3_client.copy_object.assert_any_call(
        Bucket="dst-bucket",
        Key="dst-prefix/zarr.json",
        CopySource={"Bucket": "src-bucket", "Key": "src-prefix/zarr.json"},
        RequestPayer="requester",
    )
    mock_s3_client.copy_object.assert_any_call(
        Bucket="dst-bucket",
        Key="dst-prefix/confidence/0.0",
        CopySource={"Bucket": "src-bucket", "Key": "src-prefix/confidence/0.0"},
        RequestPayer="requester",
    )
    assert mock_s3_client.copy_object.call_count == 2
