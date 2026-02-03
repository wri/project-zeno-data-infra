from typing import Optional, Tuple
import xarray as xr
import pandas as pd

from prefect import task
from pipelines.tree_cover_loss import stages


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
    return stages.load_data(
        tree_cover_loss_uri,
        pixel_area_uri,
        carbon_emissions_uri,
        tree_cover_density_uri,
        ifl_uri,
        drivers_uri,
        primary_forests_uri
    )


@task
def setup_compute(
    datasets: Tuple,
    expected_groups,
) -> Tuple:
    return stages.setup_compute(datasets, expected_groups)


@task
def postprocess_result(result: xr.DataArray) -> pd.DataFrame:
    return stages.create_result_dataframe(result)
