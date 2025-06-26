import boto3
import requests
from botocore.exceptions import ClientError


def get_latest_version(dataset):
    url = f"https://data-api.globalforestwatch.org/dataset/{dataset}/latest"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    if data["status"] != "success":
        raise ValueError(f"No latest version set for dataset {dataset}")

    return data["data"]["version"]


def s3_object_exists(bucket_name, object_key):
    try:
        s3_client = boto3.client("s3")
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise
