from typing import List, Optional

from prefect import flow
from shapely.geometry import Polygon

from pipelines.integrated_alerts.prefect_flows import integrated_alerts_common_tasks
from pipelines.utils import s3_uri_exists
from pipelines.zonal_stats.aoi_sources import custom_aois


@flow(name="Integrated alerts vector zonal stats", retries=2, retry_delay_seconds=120)
def integrated_alerts_vector_zonal_stats(
    integrated_alerts_zarr_uri: str,
    version: str,
    overwrite=False,
    bbox: Optional[Polygon] = None,
) -> List[str]:
    """Compute integrated alerts area by AOI, date, confidence and SBTN natural
    lands class for vector AOIs other than GADM, one parquet per AOI type, in the
    same schema as the GADM results. Covers the monitored custom AOIs.

    Args:
        integrated_alerts_zarr_uri: URI of the integrated alerts date_conf zarr.
        version: Integrated alerts version, used in the output S3 paths.
        overwrite: If True, recompute and overwrite existing S3 parquets.
        bbox: Optional polygon to clip the computation to, for a small local
            test run. Results are then written to local parquets
            (``{aoi_type}-integrated-alerts-{version}.parquet``).

    Returns:
        The URIs of the saved parquets.
    """
    result_uris = []
    for aoi_source in [custom_aois()]:
        if bbox is None:
            result_uri = (
                f"{integrated_alerts_common_tasks.INTEGRATED_ALERTS_PREFIX}"
                f"/{version}/{aoi_source.aoi_type}-integrated-alerts.parquet"
            )
            if not overwrite and s3_uri_exists(result_uri):
                result_uris.append(result_uri)
                continue
        else:
            # local bbox test run
            result_uri = f"{aoi_source.aoi_type}-integrated-alerts-{version}.parquet"

        integrated_alerts_common_tasks.build_vector_zonal_stats.with_options(
            name=f"build-{aoi_source.aoi_type}-zonal-stats"
        )(aoi_source, integrated_alerts_zarr_uri, version, result_uri, bbox)
        result_uris.append(result_uri)
    return result_uris
