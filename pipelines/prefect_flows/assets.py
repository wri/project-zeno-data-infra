"""Prefect asset keys for pipeline inputs and outputs, used to track lineage."""

from pathlib import Path
from urllib.parse import urlparse

from prefect.assets import Asset, AssetProperties

from pipelines.sources.source_catalog import Source


def source_asset(source: Source) -> Asset:
    return Asset(
        key=source.uri,
        properties=AssetProperties(
            name=source.title, description=source.description, url=source.url
        ),
    )


def result_asset_key(result_uri: str) -> str:
    """Asset keys need a URI scheme, so local results are keyed as file URIs."""
    if urlparse(result_uri).scheme:
        return result_uri
    return Path(result_uri).resolve().as_uri()
