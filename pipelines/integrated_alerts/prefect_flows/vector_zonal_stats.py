from typing import List, Optional, Sequence

from prefect import flow, get_run_logger
from shapely.geometry import Polygon

from pipelines.integrated_alerts.prefect_flows import integrated_alerts_common_tasks
from pipelines.utils import s3_uri_exists
from pipelines.zonal_stats.aoi_sources import AOI_SOURCE_TYPES


@flow(name="Integrated alerts vector zonal stats", retries=2, retry_delay_seconds=120)
def integrated_alerts_vector_zonal_stats(
    integrated_alerts_zarr_uri: str,
    version: str,
    aoi_types: Sequence[str],
    overwrite=False,
    bbox: Optional[Polygon] = None,
) -> List[str]:
    """Compute integrated alerts area by AOI, date, confidence and SBTN natural
    lands class for each of the AOI types given, one parquet per type, in the
    same schema as the GADM results. Types are the AOI pipeline's datasets
    (admin, protected_area, key_biodiversity_area, indigenous_land) and the
    custom AOIs (custom); admin results go under coverage/, beside the
    rasterized GADM results. Each type is computed even if another fails; the
    flow then fails, naming them.

    Args:
        integrated_alerts_zarr_uri: URI of the integrated alerts date_conf zarr.
        version: Integrated alerts version, used in the output S3 paths.
        aoi_types: The AOI types to compute.
        overwrite: If True, recompute and overwrite existing S3 parquets.
        bbox: Optional polygon to clip the computation to, for a small local
            test run. Results are then written to local parquets
            (``{aoi_type}-integrated-alerts-{version}.parquet``).

    Returns:
        The URIs of the saved parquets.
    """
    logger = get_run_logger()
    unknown = set(aoi_types) - set(AOI_SOURCE_TYPES)
    if unknown:
        raise ValueError(f"Unknown AOI types: {', '.join(sorted(unknown))}")
    result_uris, failed = [], []
    for aoi_type in [t for t in AOI_SOURCE_TYPES if t in aoi_types]:
        if bbox is None:
            directory = f"{version}/coverage" if aoi_type == "admin" else version
            result_uri = (
                f"{integrated_alerts_common_tasks.INTEGRATED_ALERTS_PREFIX}"
                f"/{directory}/{aoi_type}-integrated-alerts.parquet"
            )
            if not overwrite and s3_uri_exists(result_uri):
                result_uris.append(result_uri)
                continue
        else:
            # local bbox test run
            result_uri = f"{aoi_type}-integrated-alerts-{version}.parquet"

        task_name = f"integrated-alerts-{aoi_type}-compute-zonal-stats"
        if aoi_type == "admin":
            state = (
                integrated_alerts_common_tasks.compute_admin_zonal_stats.with_options(
                    name=task_name
                )(
                    integrated_alerts_zarr_uri,
                    version,
                    result_uri,
                    bbox,
                    return_state=True,
                )
            )
        else:
            state = (
                integrated_alerts_common_tasks.compute_vector_zonal_stats.with_options(
                    name=task_name
                )(
                    aoi_type,
                    integrated_alerts_zarr_uri,
                    version,
                    result_uri,
                    bbox,
                    return_state=True,
                )
            )
        if state.is_failed():
            logger.warning(f"{aoi_type} zonal stats failed: {state.message}")
            failed.append(aoi_type)
        else:
            result_uris.append(result_uri)

    if failed:
        raise RuntimeError(f"Zonal stats failed for {', '.join(failed)}")
    return result_uris
