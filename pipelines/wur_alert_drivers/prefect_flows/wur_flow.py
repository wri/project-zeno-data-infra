from prefect import flow, task

from pipelines.utils import get_secret
from pipelines.wur_alert_drivers import stages
from pipelines.wur_alert_drivers.data_api import DataApiClient

DATASET = "wur_integration_alert_drivers_class"
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
def create_version_task(version: str) -> None:
    stages.create_version_if_needed(
        data_api_client(), DATASET, version, SOURCE_URI, RASTER_TILE_SET_TIMEOUT_SECONDS
    )


@task
def create_cog_task(version: str) -> None:
    stages.create_cog_if_needed(
        data_api_client(), DATASET, version, COG_TIMEOUT_SECONDS
    )


@task
def set_metadata_task(version: str, content_date: str) -> None:
    stages.set_metadata(data_api_client(), DATASET, version, content_date)


@task
def set_latest_task(version: str) -> None:
    stages.set_latest(data_api_client(), DATASET, version)


@flow(
    name="WUR alert drivers update",
    log_prints=True,
    description=(
        "Create a new Data API version of the WUR integrated alert drivers dataset "
        "from the GeoTIFFs in gs://lcl_tiles/wur_alert_drivers"
    ),
)
def wur_alert_drivers_flow(
    content_date: str, version: str, is_latest: bool = False
) -> str:
    """Create a new version of wur_integration_alert_drivers_class in the Data API.

    Creates the version with its raster tile set, then a COG asset, waiting for
    each to be saved, then sets the version's metadata, and optionally marks it
    as the latest version. If the version or its COG already exist (e.g. when
    re-running after a failure), they aren't created again.

    Args:
        content_date: Date (YYYY-MM-DD) up to which the alert drivers have been
            calculated, used for the version's content date metadata.
        version: Name of the new version, e.g. v20261018.
        is_latest: If True, mark the new version as the latest version.

    Returns:
        The dataset and version created, as "dataset/version".
    """
    create_version_task(version)
    create_cog_task(version)
    set_metadata_task(version, content_date)
    if is_latest:
        set_latest_task(version)
    return f"{DATASET}/{version}"
