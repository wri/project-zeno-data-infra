from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from pipelines.utils import (
    InvalidS3UriError,
    copy_s3_directory,
    get_secret,
    parse_s3_uri,
    s3_uri_exists,
)


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

    copy_s3_directory("s3://src-bucket/src-prefix", "s3://dst-bucket/dst-prefix")

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


@patch("pipelines.utils.boto3.client")
def test_s3_uri_exists_true(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.head_object.return_value = {}

    assert s3_uri_exists("s3://my-bucket/my/key.zarr") is True
    mock_s3.head_object.assert_called_once_with(
        Bucket="my-bucket", Key="my/key.zarr", RequestPayer="requester"
    )


@patch("pipelines.utils.boto3.client")
def test_s3_uri_exists_false(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "404"}}, "HeadObject"
    )

    assert s3_uri_exists("s3://my-bucket/missing/key.zarr") is False


@patch("pipelines.utils.boto3.client")
def test_s3_uri_exists_other_error(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "403"}}, "HeadObject"
    )

    with pytest.raises(ClientError):
        s3_uri_exists("s3://my-bucket/forbidden/key.zarr")
