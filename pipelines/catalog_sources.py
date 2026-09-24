"""Resolves pipeline sources from a Portolan (STAC) catalog.

Collections publish an https href for discovery and an S3 alternate, which the
pipelines read so requester-pays credentials apply.
"""

import json

import fsspec


def source_uri(catalog_root: str, collection_id: str) -> str:
    with fsspec.open(f"{catalog_root}/{collection_id}/collection.json") as file:
        collection = json.load(file)
    return collection["assets"]["data"]["alternate"]["s3"]["href"]
