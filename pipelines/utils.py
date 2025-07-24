import boto3
from botocore.exceptions import ClientError
import re

class InvalidS3UriError(ValueError):
    pass

def s3_uri_exists(s3_uri):
    pattern = r"^s3://([^/]+)/(.*)$"
    match = re.match(pattern, s3_uri)
    if not match:
        raise InvalidS3UriError
    bucket_name, object_key = match.groups()
    try:
        s3_client = boto3.client("s3")
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise
