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

LoaderType = Callable[[str], Tuple[xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]

def _s3_loader(
    dist_zarr_uri: str,
) -> Tuple[xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset]:

    dist_alerts = xr.open_zarr(dist_zarr_uri)

    country = xr.open_zarr(country_zarr_uri)
    country_aligned = xr.align(dist_alerts, country, join='left')[1]
    region = xr.open_zarr(region_zarr_uri)
    region_aligned = xr.align(dist_alerts, region, join='left')[1]
    subregion = xr.open_zarr(subregion_zarr_uri)
    subregion_aligned = xr.align(dist_alerts, subregion, join='left')[1]

    natural_lands = xr.open_zarr(
        f's3://{DATA_LAKE_BUCKET}/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes.zarr'
    )
    natural_lands_aligned = xr.align(dist_alerts, natural_lands, join='left')[1]

    return (
        dist_alerts,
        country_aligned,
        region_aligned,
        subregion_aligned,
        natural_lands_aligned
    )

def _parquet_saver(alerts_count_df: pd.DataFrame, results_uri: str) -> None:
    alerts_count_df.to_parquet(results_uri, index=False)


def gadm_dist_alerts_by_natural_lands(
    zarr_uri: str,
    version: str,
    loader: LoaderType = _s3_loader,
    groups: Optional[ExpectedGroupsType] = None,
    saver: SaverType = _parquet_saver,
    overwrite: bool = False
) -> str:
    """Run DIST alerts analysis by natural lands using Dask to create parquet, upload to S3 and return URI."""
    logging.getLogger("distributed.client").setLevel(logging.ERROR)

    results_key = f"umd_glad_dist_alerts/{version}/tabular/epsg-4326/zonal_stats/dist_alerts_by_adm2_natural_lands.parquet"
    results_uri = f"s3://{DATA_LAKE_BUCKET}/{results_key}"

    if not overwrite and s3_object_exists(DATA_LAKE_BUCKET, results_key):
        return results_uri

    return pipe(
        loader(zarr_uri),
        lambda d: _setup(*d, groups),
        lambda s: _compute(*s),
        _create_data_frame,
        lambda df: _save_results(df, version, saver, results_uri),
    )

def _setup(
    dist_alerts: xr.Dataset,
    country: xr.Dataset,
    region: xr.Dataset,
    subregion: xr.Dataset,
    dist_natural_lands: xr.Dataset,
    expected_groups: Tuple,
) -> Tuple:
    groups = (
        (
            np.arange(894),
            np.arange(86),
            np.arange(854),
            np.arange(22), # natural lands categories
            # we should get this from metadata which includes `content_date_range`
            np.arange(731, 1590),
            [1, 2, 3],
        )
        if expected_groups is None
        else expected_groups
    )
    return (
        dist_alerts.confidence,
        (
            country.band_data.rename("country"),
            region.band_data.rename("region"),
            subregion.band_data.rename("subregion"),
            dist_natural_lands.band_data.rename("natural_lands"),
            dist_alerts.alert_date,
            dist_alerts.confidence,
        ),
        groups,
    )


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
