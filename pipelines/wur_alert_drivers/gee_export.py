"""Google Earth Engine step that creates the WUR alert drivers GeoTIFFs in
gs://lcl_tiles/wur_alert_drivers from the drivers data that WUR publishes (as GEE
assets) about once a month.

For each region, WUR's historical drivers image is blended with its latest one,
the class and date are encoded into one int32 value (class * 100000 + days since
2014-12-31), and the result is exported to GCS as tiled COG GeoTIFFs.

The export logic works against a GeeClient (or an in-memory fake in tests). Each
region's export task has a name that includes the version, so a re-run skips
regions whose export already succeeded and waits on exports that are still
running, rather than starting them again.
"""

import time
from datetime import date, timedelta
from typing import Callable

from pipelines.integrated_alerts.register_gee_asset import PROJECT_CONFIG
from pipelines.utils import get_secret

REGIONS = ("afr", "sa", "sea")
GEE_PROJECT = "landandcarbon"
BUCKET = "lcl_tiles"
FOLDER = "wur_alert_drivers"
TEST_SUBFOLDER = "test"
BASE_YEAR = 2015

# WUR's historical asset covers 2022-2025 and won't be updated. All alerts from
# 2026 on are in the monthly "latest" mosaics, which start at LATEST_START (the
# end date in their names is the day after content_date).
LATEST_START = "20260101"
HISTORICAL_PERIOD = "202201_202512"

# Earth Engine operation states (operation["metadata"]["state"]).
SUCCEEDED = "SUCCEEDED"
ACTIVE_STATES = ("PENDING", "RUNNING")
FAILED_STATES = ("FAILED", "CANCELLING", "CANCELLED")


class ExportError(Exception):
    """Raised when a GEE export fails, or doesn't finish in time."""


def latest_end(content_date: str) -> str:
    """End date (YYYYMMDD, exclusive) in the name of WUR's latest asset: the day
    after content_date (YYYY-MM-DD), the last day included in the drivers."""
    return (date.fromisoformat(content_date) + timedelta(days=1)).strftime("%Y%m%d")


def historical_asset_path(region: str) -> str:
    return (
        "projects/wurnrt-drivers/assets/distribution/"
        f"driverclassification_{region}_{HISTORICAL_PERIOD}"
    )


def latest_asset_path(region: str, content_date: str) -> str:
    return (
        f"projects/wurnrt-drivers/assets/operational_postprocessed_{region}_collection/"
        f"driverclassification_postprocessed_{region}_mosaic_"
        f"{LATEST_START}_{latest_end(content_date)}"
    )


def file_name_prefix(region: str, test: bool = False) -> str:
    """GCS object name prefix of the region's exported GeoTIFFs (GEE adds the
    tile offsets and .tif)."""
    folder = f"{FOLDER}/{TEST_SUBFOLDER}" if test else FOLDER
    return f"{folder}/wur_alert_drivers_encoded_{region}"


def task_description(region: str, version: str, test: bool = False) -> str:
    suffix = "_test" if test else ""
    return f"wur_alert_drivers_encoded_{region}_{version}{suffix}"


def encode_driver_alerts(image):
    """Encode the image's class and date (YYDDD) bands as one int32 band:
    class * 100000 + days since 2014-12-31."""
    class_band = image.select("class")
    original_date = image.select("date")  # YYDDD, e.g. 26001 = 2026 day 1
    year_of_data = original_date.add(2000000).divide(1000).floor()
    days = year_of_data.subtract(BASE_YEAR).multiply(365)
    for y in range(BASE_YEAR, 2050):  # one extra day per leap year before the data year
        if y % 4 == 0:
            days = days.add(year_of_data.gt(y))
    doy = original_date.subtract(year_of_data.subtract(2000).multiply(1000))
    days = days.add(doy)
    encoded = class_band.multiply(100000).add(days).toInt32()
    return encoded.rename("alert_drivers_encoded")


class GeeClient:
    """Earth Engine and GCS access as the landandcarbon service account."""

    def __init__(self):
        import ee
        from google.cloud import storage

        config = PROJECT_CONFIG[GEE_PROJECT]
        credentials = ee.ServiceAccountCredentials(
            config["service_account"], key_data=get_secret(config["secret_id"])
        )
        ee.Initialize(credentials, project=GEE_PROJECT)
        self._ee = ee
        self._bucket = storage.Client(
            project=GEE_PROJECT, credentials=credentials
        ).bucket(BUCKET)

    def list_tasks(self) -> list[dict]:
        return [_task(op) for op in self._ee.data.listOperations()]

    def get_task(self, name: str) -> dict:
        return _task(self._ee.data.getOperation(name))

    def delete_tifs(self, prefix: str) -> int:
        """Delete the .tif objects whose names start with prefix, and return how
        many were deleted."""
        blobs = [
            b for b in self._bucket.list_blobs(prefix=prefix) if b.name.endswith(".tif")
        ]
        for blob in blobs:
            blob.delete()
        return len(blobs)

    def start_export(
        self, region: str, content_date: str, description: str, prefix: str
    ) -> str:
        """Start the region's export to gs://BUCKET/prefix*.tif, and return the
        export's operation name."""
        ee = self._ee
        historical = ee.Image(historical_asset_path(region))
        latest = ee.Image(latest_asset_path(region, content_date))
        merged = historical.blend(latest)
        proj = merged.projection().getInfo()
        task = ee.batch.Export.image.toCloudStorage(
            image=encode_driver_alerts(merged),
            description=description,
            bucket=BUCKET,
            fileNamePrefix=prefix,
            scale=10,
            maxPixels=1e13,
            region=merged.geometry(),
            crs=proj["crs"],
            crsTransform=proj["transform"],
            fileFormat="GeoTIFF",
            formatOptions={"cloudOptimized": True},
        )
        task.start()
        return task.name


def _task(operation: dict) -> dict:
    metadata = operation.get("metadata", {})
    return {
        "name": operation["name"],
        "description": metadata.get("description"),
        "state": metadata.get("state"),
        "error": operation.get("error", {}).get("message"),
    }


def start_exports_if_needed(
    client, version: str, content_date: str, test: bool = False
) -> list[str]:
    """Start each region's export, unless one for this version already succeeded
    or is still running. Returns the operation names of the exports to wait on.

    Before starting a region's export, its old GeoTIFFs are deleted, so tiles
    from an earlier export (which may have a different tiling) can't be mixed in.
    """
    tasks = client.list_tasks()
    to_wait_on = []
    for region in REGIONS:
        description = task_description(region, version, test)
        matching = [t for t in tasks if t["description"] == description]
        active = [t for t in matching if t["state"] in ACTIVE_STATES]
        if any(t["state"] == SUCCEEDED for t in matching):
            print(f"Export {description} already succeeded, not starting it again")
        elif active:
            print(
                f"Export {description} is already {active[0]['state']}, waiting on it"
            )
            to_wait_on.append(active[0]["name"])
        else:
            prefix = file_name_prefix(region, test)
            deleted = client.delete_tifs(prefix)
            print(f"Deleted {deleted} old GeoTIFFs gs://{BUCKET}/{prefix}*.tif")
            name = client.start_export(region, content_date, description, prefix)
            print(f"Started export {description}: {name}")
            to_wait_on.append(name)
    return to_wait_on


def wait_for_exports(
    client,
    names: list[str],
    timeout_seconds: float,
    poll_seconds: float = 300,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Poll the exports until they have all succeeded.

    Raises ExportError as soon as any export has failed or been cancelled, or if
    some are still unfinished after timeout_seconds. Exports still running are
    left running, so a re-run can wait on them.
    """
    deadline = clock() + timeout_seconds
    pending = list(names)
    while True:
        tasks = [client.get_task(name) for name in pending]
        failed = [t for t in tasks if t["state"] in FAILED_STATES]
        if failed:
            raise ExportError(f"GEE export(s) failed: {_describe(failed)}")
        pending = [t["name"] for t in tasks if t["state"] != SUCCEEDED]
        if not pending:
            return
        if clock() >= deadline:
            unfinished = [t for t in tasks if t["state"] != SUCCEEDED]
            raise ExportError(
                f"GEE export(s) unfinished after {timeout_seconds}s: "
                f"{_describe(unfinished)}"
            )
        print(f"Waiting for GEE exports: {_describe(tasks)}")
        sleep(poll_seconds)


def _describe(tasks: list[dict]) -> str:
    parts = []
    for t in tasks:
        error = f" ({t['error']})" if t.get("error") else ""
        parts.append(f"{t['description']} {t['state']}{error}")
    return ", ".join(parts)
