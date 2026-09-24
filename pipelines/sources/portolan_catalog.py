"""Sources registered in a Portolan (STAC) catalog.

Each collection publishes an https href for discovery and an S3 alternate,
which is what pipelines read so requester-pays credentials apply.
"""

import json

import fsspec

from pipelines.sources.source_catalog import Source, SourceCatalog


class PortolanCatalog(SourceCatalog):
    def __init__(self, root: str):
        self.root = root

    def source(self, source_id: str) -> Source:
        with fsspec.open(f"{self.root}/{source_id}/collection.json") as file:
            collection = json.load(file)
        data = collection["assets"]["data"]
        return Source(
            uri=data["alternate"]["s3"]["href"],
            title=collection["title"],
            description=collection["description"],
            url=data["href"],
        )
