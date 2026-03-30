from typing import Optional, Tuple

import pandas as pd
import xarray as xr
from prefect import task
from shapely import Polygon

from pipelines.tree_cover_loss import stages
from pipelines.tree_cover_loss.create_zarr import create_zarrs as create_zarrs_func


@task
def create_zarrs(overwrite=False) -> dict[str, str]:
    return create_zarrs_func(overwrite=overwrite)


@task
def load_data(
    tree_cover_loss_uri: str,
    pixel_area_uri: Optional[str] = None,
    carbon_emissions_uri: Optional[str] = None,
    tree_cover_density_uri: Optional[str] = None,
    ifl_uri: Optional[str] = None,
    drivers_uri: Optional[str] = None,
    primary_forests_uri: Optional[str] = None,
    natural_forests_uri: Optional[str] = None,
    tree_cover_loss_from_fires_uri: Optional[str] = None,
    bbox: Optional[Polygon] = None,
    group: Optional[str] = None,
) -> Tuple:
    return stages.load_data(
        tree_cover_loss_uri,
        pixel_area_uri,
        carbon_emissions_uri,
        tree_cover_density_uri,
        ifl_uri,
        drivers_uri,
        primary_forests_uri,
        natural_forests_uri,
        tree_cover_loss_from_fires_uri,
        bbox,
        group=group,
    )


@task
def setup_compute(
    datasets: Tuple,
    expected_groups,
    contextual_name: Optional[str] = None,
) -> Tuple:
    return stages.setup_compute(datasets, expected_groups)


@task
def postprocess_result(result: xr.DataArray) -> pd.DataFrame:
    return stages.postprocess_result(result)


@task
def qc_against_validation_source(
    result_df: pd.DataFrame, version: Optional[str] = None
) -> bool:
    return stages.qc_against_validation_source(result_df=result_df, version=version)
