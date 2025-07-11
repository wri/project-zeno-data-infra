import logging
from functools import reduce
from typing import Callable, Tuple, Optional
import numpy as np
import pandas as pd
import xarray as xr
from flox import ReindexArrayType, ReindexStrategy
from flox.xarray import xarray_reduce

from .check_for_new_alerts import s3_object_exists
from ..globals import DATA_LAKE_BUCKET, country_zarr_uri, region_zarr_uri, subregion_zarr_uri

LoaderType = Callable[[str, Optional[str]], Tuple[xr.Dataset, ...]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]

def _s3_loader(
    dist_zarr_uri: str,
    contextual_uri: Optional[str],
) -> Tuple[xr.Dataset, ...]:
    """Load in the Dist alert Zarr, the GADM zarrs, and possibly a contextual layer zarr"""

    dist_alerts = xr.open_zarr(dist_zarr_uri)

    country = xr.open_zarr(country_zarr_uri)
    country_aligned = xr.align(dist_alerts, country, join='left')[1]
    region = xr.open_zarr(region_zarr_uri)
    region_aligned = xr.align(dist_alerts, region, join='left')[1]
    subregion = xr.open_zarr(subregion_zarr_uri)
    subregion_aligned = xr.align(dist_alerts, subregion, join='left')[1]

    if contextual_uri is not None:
        contextual_layer = xr.open_zarr(contextual_uri)
        contextual_layer_aligned = xr.align(dist_alerts, contextual_layer, join='left')[1]
    else:
        contextual_layer_aligned = None

    return (
        dist_alerts,
        country_aligned,
        region_aligned,
        subregion_aligned,
        contextual_layer_aligned
    )

def _parquet_saver(alerts_count_df: pd.DataFrame, results_uri: str) -> None:
    alerts_count_df.to_parquet(results_uri, index=False)


def gadm_dist_alerts_by_natural_lands(
    dist_zarr_uri: str,
    dist_version: str,
    loader: LoaderType = _s3_loader,
    groups: Optional[ExpectedGroupsType] = None,
    saver: SaverType = _parquet_saver,
    overwrite: bool = False
) -> str:
    """Run DIST alerts analysis by natural lands using Dask to create parquet, upload to S3 and return URI."""
    logging.getLogger("distributed.client").setLevel(logging.ERROR)

    contextual_uri = f's3://{DATA_LAKE_BUCKET}/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes.zarr'

    expected_groups = (
        (
            np.arange(894),        # country ISO codes
            np.arange(86),         # region codes
            np.arange(854),        # subregion codes
            np.arange(22),         # natural lands categories
            np.arange(731, 1590),  # date range
            [1, 2, 3],             # confidence values
        )
        if groups is None
        else groups
    )

    contextual_column_name = 'natural_lands'

    results_key = f"umd_glad_dist_alerts/{dist_version}/tabular/epsg-4326/zonal_stats/dist_alerts_by_adm2_natural_lands.parquet"
    results_uri = f"s3://{DATA_LAKE_BUCKET}/{results_key}"

    if not overwrite and s3_object_exists(DATA_LAKE_BUCKET, results_key):
        return results_uri

    return pipe(
        loader(dist_zarr_uri, contextual_uri),
        lambda d: _setup(d, expected_groups, contextual_column_name),
        lambda s: _compute(*s),
        _create_data_frame,
        lambda df: _save_results(df, dist_version, saver, results_uri),
    )

def _setup(
    datasets: Tuple[xr.Dataset, ...],
    expected_groups: Optional[ExpectedGroupsType],
    contextual_column_name: Optional[str]
) -> Tuple:
    """Setup the arguments for the xrarray reduce on dist alerts"""
    dist_alerts, country, region, subregion, contextual_layer = datasets

    mask = dist_alerts.confidence
    groupbys: Tuple[xr.Dataset, ...] = (
        country.band_data.rename("country"),
        region.band_data.rename("region"),
        subregion.band_data.rename("subregion"),
        dist_alerts.alert_date,
        dist_alerts.confidence,
    )
    if contextual_layer is not None:
        groupbys = groupbys[:2] + (contextual_layer.band_data.rename(contextual_column_name),) + groupbys[2:]

    return (mask, groupbys, expected_groups)


def _compute(reduce_mask: xr.DataArray, reduce_groupbys: Tuple, expected_groups: Tuple):
    print("Starting reduce")
    alerts_count = xarray_reduce(
        reduce_mask,
        *reduce_groupbys,
        func="count",
        expected_groups=expected_groups,
        reindex=ReindexStrategy(
            blockwise=False, array_type=ReindexArrayType.SPARSE_COO
        ),
        fill_value=0,
    ).compute()
    print("Finished reduce")
    return alerts_count

def _create_data_frame(alerts_count: xr.Dataset) -> pd.DataFrame:
    sparse_data = alerts_count.data
    dim_names = alerts_count.dims
    indices = sparse_data.coords
    values = sparse_data.data

    coord_dict = {
        dim: alerts_count.coords[dim].values[idx]
        for dim, idx in zip(dim_names, indices)
    }
    coord_dict["value"] = values

    return pd.DataFrame(coord_dict)

def _save_results(
    alerts_count_df: pd.DataFrame, dist_version: str, saver: Callable, results_uri: str
) -> str:
    print("Starting parquet")
    saver(alerts_count_df, results_uri)
    print("Finished parquet")
    return results_uri


def pipe(value, *functions):
    return reduce(lambda v, f: f(v), functions, value)
