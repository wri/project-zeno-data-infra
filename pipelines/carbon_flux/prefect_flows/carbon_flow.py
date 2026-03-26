from typing import Optional, Tuple

from prefect import flow
from shapely.geometry import box

from pipelines.globals import (
    ANALYTICS_BUCKET,
)
from pipelines.prefect_flows import common_tasks
from pipelines.carbon_flux.prefect_flows import carbon_tasks
from pipelines.carbon_flux.prefect_flows.carbon import gadm_carbon_flux
from pipelines.utils import s3_uri_exists


@flow(name="Carbon flux")
def gadm_carbon_flux_flow(
    version: Optional[str] = None,
    overwrite=False,
    bbox: Optional[Tuple[float, float, float, float]] = None
):
    result_uri = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/admin-carbon.parquet"

    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    if bbox is not None:
        bbox = box(*bbox)

    result_df = gadm_carbon_flux(carbon_tasks.CarbonFluxPrefectTasks, version=version, bbox=bbox)

    result_uri = common_tasks.save_result.with_options(
        name="carbon-flux-save-result"
    )(result_df, result_uri)

    return result_uri


def main(overwrite=False):
    gadm_carbon_flux_flow(overwrite=overwrite)


if __name__ == "__main__":
    main(overwrite=False)
