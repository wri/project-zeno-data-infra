from prefect import flow, task

from pipelines.data_api import DataApiClient
from pipelines.utils import get_secret
from pipelines.wur_alert_drivers import stages

# Both datasets are created from the same GeoTIFFs, with the same version name.
# wur_integration_alert_drivers_class has just the driver class (with a COG), and
# wur_alert_drivers keeps the full driver/date values (for raster analysis and
# download only, so no COG).
CLASS_DATASET = "wur_integration_alert_drivers_class"
DRIVER_DATE_DATASET = "wur_alert_drivers"
# New alert drivers data is put here (by hand, about monthly) before each run.
SOURCE_URI = "gs://lcl_tiles/wur_alert_drivers"
DATA_API_TOKEN_SECRET_ID = "gfw-api/token"
RASTER_TILE_SET_TIMEOUT_SECONDS = 3 * 60 * 60
# COG generation is a lot slower than creating the raster tile set.
COG_TIMEOUT_SECONDS = 6 * 60 * 60


def data_api_client() -> DataApiClient:
    # Each task makes its own client, so the token is never a task parameter
    # (which Prefect would log and hash for its cache key).
    return DataApiClient(token=get_secret(DATA_API_TOKEN_SECRET_ID))


@task
def create_version_task(dataset: str, version: str, body: dict) -> None:
    stages.create_version_if_needed(
        data_api_client(), dataset, version, body, RASTER_TILE_SET_TIMEOUT_SECONDS
    )


@task
def create_cog_task(version: str) -> None:
    stages.create_cog_if_needed(
        data_api_client(), CLASS_DATASET, version, COG_TIMEOUT_SECONDS
    )


@task
def set_metadata_task(version: str, content_date: str) -> None:
    stages.set_metadata(data_api_client(), CLASS_DATASET, version, content_date)


@task
def set_latest_task(dataset: str, version: str) -> None:
    stages.set_latest(data_api_client(), dataset, version)


@flow(
    name="WUR alert drivers update",
    log_prints=True,
    description=(
        "Create new Data API versions of the WUR alert drivers datasets "
        "from the GeoTIFFs in gs://lcl_tiles/wur_alert_drivers"
    ),
)
def wur_alert_drivers_flow(
    content_date: str, version: str, is_latest: bool = False
) -> list[str]:
    """Create new versions of wur_alert_drivers and
    wur_integration_alert_drivers_class in the Data API.

    First creates the wur_alert_drivers version with its raster tile set. Then
    creates the wur_integration_alert_drivers_class version with its raster tile
    set, then a COG asset, and sets that version's metadata. It waits for each
    version's or COG's assets to be saved before going on. Optionally, both
    versions are then marked as latest. If a version or the COG already exist
    (e.g. when re-running after a failure), they aren't created again.

    Args:
        content_date: Date (YYYY-MM-DD) up to which the alert drivers have been
            calculated, used for wur_integration_alert_drivers_class's content
            date metadata.
        version: Name of the new versions of both datasets, e.g. v20261018.
        is_latest: If True, mark both new versions as the latest versions.

    Returns:
        The datasets and versions created, as "dataset/version".
    """
    create_version_task(
        DRIVER_DATE_DATASET,
        version,
        stages.driver_date_raster_tile_set_body(SOURCE_URI),
    )
    create_version_task(
        CLASS_DATASET, version, stages.class_raster_tile_set_body(SOURCE_URI)
    )
    create_cog_task(version)
    set_metadata_task(version, content_date)
    if is_latest:
        set_latest_task(DRIVER_DATE_DATASET, version)
        set_latest_task(CLASS_DATASET, version)
    return [f"{DRIVER_DATE_DATASET}/{version}", f"{CLASS_DATASET}/{version}"]
