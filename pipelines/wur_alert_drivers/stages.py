"""Steps for creating a new version of the WUR integrated alert drivers dataset in
the GFW Data API.

Each step works against a Data API client (see pipelines.data_api.DataApiClient),
so it can be tested with an in-memory fake. The two steps that create assets skip
creation if the asset already exists, so a failed run can be resumed with the same
version.
"""

import time
from typing import Callable

# Start of the alert drivers data, which always stays the same.
CONTENT_START_DATE = "2014-12-31"
RASTER_TILE_SET = "Raster tile set"
COG = "COG"


class AssetCreationError(Exception):
    """Raised when a Data API asset fails, or isn't saved in time."""


def raster_tile_set_body(source_uri: str) -> dict:
    """Body for creating the version and its raster tile set from GeoTIFFs."""
    return {
        "creation_options": {
            "pixel_meaning": "class",
            "data_type": "uint8",
            "calc": "A // 100000",
            "band_count": 1,
            "no_data": 0,
            "resampling": "nearest",
            "grid": "10/100000",
            "source_type": "raster",
            "source_driver": "GeoTIFF",
            "source_uri": [source_uri],
        }
    }


def cog_asset_body(source_asset_id: str) -> dict:
    """Body for creating a COG asset from the raster tile set source_asset_id."""
    return {
        "asset_type": COG,
        "creation_options": {
            "implementation": "class",
            "source_asset_id": source_asset_id,
            "resampling": "mode",
            "block_size": 1024,
            "compute_stats": False,
            "export_to_gee": False,
            "no_data": None,
        },
    }


def metadata_body(content_date: str) -> dict:
    """Body for setting the version's metadata, given content_date (YYYY-MM-DD),
    the date up to which the alert drivers have been calculated."""
    return {
        "metadata": {
            "content_date": content_date,
            "content_date_range": {
                "start_date": CONTENT_START_DATE,
                "end_date": content_date,
            },
            "last_update": content_date,
        }
    }


def wait_for_assets_saved(
    client,
    dataset: str,
    version: str,
    timeout_seconds: float,
    poll_seconds: float = 60,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> list[dict]:
    """Poll the version's assets until they are all saved, and return them.

    Raises AssetCreationError as soon as any asset has failed, or if some are
    still pending after timeout_seconds.
    """
    deadline = clock() + timeout_seconds
    while True:
        assets = client.get_assets(dataset, version)
        failed = [a for a in assets if a["status"] == "failed"]
        if failed:
            raise AssetCreationError(
                f"{dataset}/{version} asset(s) failed: {_describe(failed)}"
            )
        if assets and all(a["status"] == "saved" for a in assets):
            return assets
        if clock() >= deadline:
            pending = [a for a in assets if a["status"] != "saved"]
            raise AssetCreationError(
                f"{dataset}/{version} asset(s) still pending after "
                f"{timeout_seconds}s: {_describe(pending)}"
            )
        print(f"Waiting for {dataset}/{version} assets: {_describe(assets)}")
        sleep(poll_seconds)


def create_version_if_needed(
    client,
    dataset: str,
    version: str,
    source_uri: str,
    timeout_seconds: float,
    **wait_options,
) -> list[dict]:
    """Create the version (and its raster tile set) from source_uri, unless it
    already exists, then wait until its assets are saved."""
    if client.get_version(dataset, version) is None:
        client.create_version(dataset, version, raster_tile_set_body(source_uri))
    else:
        print(f"{dataset}/{version} already exists, not creating it again")
    return wait_for_assets_saved(
        client, dataset, version, timeout_seconds, **wait_options
    )


def create_cog_if_needed(
    client, dataset: str, version: str, timeout_seconds: float, **wait_options
) -> list[dict]:
    """Create a COG asset from the version's raster tile set, unless the version
    already has a COG, then wait until its assets are saved."""
    assets = client.get_assets(dataset, version)
    if any(a["asset_type"] == COG for a in assets):
        print(f"{dataset}/{version} already has a COG, not creating it again")
    else:
        (tile_set,) = [a for a in assets if a["asset_type"] == RASTER_TILE_SET]
        client.create_asset(dataset, version, cog_asset_body(tile_set["asset_id"]))
    return wait_for_assets_saved(
        client, dataset, version, timeout_seconds, **wait_options
    )


def set_metadata(client, dataset: str, version: str, content_date: str) -> None:
    client.update_version(dataset, version, metadata_body(content_date))


def set_latest(client, dataset: str, version: str) -> None:
    client.update_version(dataset, version, {"is_latest": True})


def _describe(assets: list[dict]) -> str:
    return ", ".join(f"{a['asset_type']} {a['status']}" for a in assets)
