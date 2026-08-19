import re
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class InvalidS3UriError(ValueError):
    pass


def parse_s3_uri(s3_uri):
    pattern = r"^s3://([^/]+)/(.*)$"
    match = re.match(pattern, s3_uri)
    if not match:
        raise InvalidS3UriError
    return match.groups()


def s3_uri_exists(s3_uri):
    bucket_name, object_key = parse_s3_uri(s3_uri)
    try:
        s3_client = boto3.client("s3")
        s3_client.head_object(
            Bucket=bucket_name, Key=object_key, RequestPayer="requester"
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise


def copy_s3_directory(src_uri, dst_uri, max_workers=100):
    """Recursively copy every object under src_uri to dst_uri using
    parallel server-side S3 copies, so data never leaves S3."""
    src_bucket, src_prefix = parse_s3_uri(src_uri)
    dst_bucket, dst_prefix = parse_s3_uri(dst_uri)
    if not src_prefix.endswith("/"):
        src_prefix += "/"
    if not dst_prefix.endswith("/"):
        dst_prefix += "/"

    # boto3's default connection pool (10) would otherwise serialize most of
    # the thread pool's requests, since copy_object holds a pooled connection
    # for the round trip; size it to the concurrency we actually want.
    s3_client = boto3.client("s3", config=Config(max_pool_connections=max_workers))
    paginator = s3_client.get_paginator("list_objects_v2")

    def copy_one(key):
        dst_key = dst_prefix + key[len(src_prefix):]
        s3_client.copy_object(
            Bucket=dst_bucket,
            Key=dst_key,
            CopySource={"Bucket": src_bucket, "Key": key},
            RequestPayer="requester",
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(copy_one, obj["Key"])
            for page in paginator.paginate(
                Bucket=src_bucket, Prefix=src_prefix, RequestPayer="requester"
            )
            for obj in page.get("Contents", [])
        ]
        for future in futures:
            future.result()


def get_secret(secret_id):
    """Fetch a secret's string value from AWS Secrets Manager."""
    client = boto3.client("secretsmanager")
    return client.get_secret_value(SecretId=secret_id)["SecretString"]
