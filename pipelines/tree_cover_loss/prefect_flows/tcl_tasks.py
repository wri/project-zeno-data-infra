from typing import Optional, Tuple

import pandas as pd
import xarray as xr
from prefect import task

from pipelines.prefect_flows.common_tasks import compute_zonal_stat
from pipelines.tree_cover_loss.stages import TreeCoverLossTasks


@task
def load_data(
    tree_cover_loss_uri: str,
    pixel_area_uri: Optional[str] = None,
    carbon_emissions_uri: Optional[str] = None,
    tree_cover_density_uri: Optional[str] = None,
    ifl_uri: Optional[str] = None,
    drivers_uri: Optional[str] = None,
    primary_forests_uri: Optional[str] = None,
) -> Tuple:
    return TreeCoverLossTasks.load_data(
        tree_cover_loss_uri,
        pixel_area_uri,
        carbon_emissions_uri,
        tree_cover_density_uri,
        ifl_uri,
        drivers_uri,
        primary_forests_uri,
    )


@task
def setup_compute(
    datasets: Tuple,
    expected_groups,
    contextual_name: Optional[str] = None,
) -> Tuple:
    return TreeCoverLossTasks.setup_compute(datasets, expected_groups)


@task
def postprocess_result(result: xr.DataArray) -> pd.DataFrame:
    return TreeCoverLossTasks.create_result_dataframe(result)


class TreeCoverLossPrefectTasks:
    load_data = load_data.with_options(name="area-emissions-by-tcl-load-data")
    setup_compute = setup_compute.with_options(
        name="set-up-area-emissions-by-tcl-compute"
    )
    compute_zonal_stat = compute_zonal_stat.with_options(
        name="area-emissions-by-tcl-compute-zonal-stats"
    )
    postprocess_result = postprocess_result.with_options(
        name="area-emissions-by-tcl-postprocess-result"
    )
