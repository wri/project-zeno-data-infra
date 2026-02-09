from typing import Optional, Tuple

from prefect import flow
from shapely.geometry import box

from pipelines.globals import (
    ANALYTICS_BUCKET,
)
from pipelines.prefect_flows import common_tasks
from pipelines.tree_cover_loss.prefect_flows import tcl_tasks
from pipelines.tree_cover_loss.prefect_flows.tcl import umd_tree_cover_loss
from pipelines.utils import s3_uri_exists


@flow
def umd_tree_cover_loss_flow(
    overwrite=False, bbox: Optional[Tuple[float, float, float, float]] = None
):
    result_uri = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/admin-tree-cover-loss-emissions-2001-2024.parquet"

    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    if bbox is not None:
        bbox = box(*bbox)

    result_df = umd_tree_cover_loss(tcl_tasks.TreeCoverLossPrefectTasks, bbox=bbox)

    result_uri = common_tasks.save_result.with_options(
        name="area-emissions-by-tcl-save-result"
    )(result_df, result_uri)

    return result_uri


def main(overwrite=False):
    umd_tree_cover_loss_flow(overwrite=overwrite)


if __name__ == "__main__":
    main(overwrite=False)
