from prefect import flow

from pipelines.globals import (
    ANALYTICS_BUCKET,
)
from pipelines.prefect_flows import common_tasks
from pipelines.tree_cover_loss.prefect_flows import tcl_tasks
from pipelines.tree_cover_loss.prefect_flows.tcl import umd_tree_cover_loss
from pipelines.utils import s3_uri_exists


@flow
def umd_tree_cover_loss_flow(overwrite=False):
    result_uri = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/admin-tree-cover-loss-emissions-2001-2024.parquet"

    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    result_df = umd_tree_cover_loss(task=tcl_tasks.TreeCoverLossPrefectTasks)

    result_uri = common_tasks.save_result.with_options(
        name="area-emissions-by-tcl-save-result"
    )(result_df, result_uri)

    return result_uri


def main(overwrite=False):
    umd_tree_cover_loss_flow(overwrite=overwrite)


if __name__ == "__main__":
    main(overwrite=False)
