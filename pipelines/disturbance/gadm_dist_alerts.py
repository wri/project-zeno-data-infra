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

LoaderType = Callable[[str], Tuple[xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]


def _s3_loader(
    dist_zarr_uri: str,
) -> Tuple[xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset]:

    dist_alerts = xr.open_zarr(dist_zarr_uri)

    country = xr.open_zarr(country_zarr_uri)
    country_from_clipped = xr.align(dist_alerts, country, join='left')[1]
    region = xr.open_zarr(region_zarr_uri)
    region_from_clipped = xr.align(dist_alerts, region, join='left')[1]
    subregion = xr.open_zarr(subregion_zarr_uri)
    subregion_from_clipped = xr.align(dist_alerts, subregion, join='left')[1]
    return (
        dist_alerts,
        country_from_clipped,
        region_from_clipped,
        subregion_from_clipped
    )


def _parquet_saver(alerts_count_df: pd.DataFrame, results_uri: str) -> None:
    alerts_count_df.to_parquet(results_uri, index=False)


def gadm_dist_alerts(
    dist_zarr_uri: str,
    dist_version: str,
    loader: LoaderType = _s3_loader,
    groups: Optional[ExpectedGroupsType] = None,
    saver: SaverType = _parquet_saver,
    overwrite: bool = False
) -> str:
    """Count DIST alerts by GADM boundary, confidence, and date, and export grouped results to a Parquet file in S3."""
    logging.getLogger("distributed.client").setLevel(logging.ERROR)

    results_key = f"umd_glad_dist_alerts/{dist_version}/tabular/epsg-4326/zonal_stats/dist_alerts_by_adm2.parquet"
    results_uri = f"s3://{DATA_LAKE_BUCKET}/{results_key}"

    if not overwrite and s3_object_exists(DATA_LAKE_BUCKET, results_key):
        return results_uri

    return pipe(
        loader(dist_zarr_uri),
        lambda d: _setup(*d, groups),
        lambda s: _compute(*s),
        _create_data_frame,
        lambda df: _save_results(df, dist_version, saver, results_uri),
    )


def _setup(
    dist_alerts: xr.Dataset,
    country: xr.Dataset,
    region: xr.Dataset,
    subregion: xr.Dataset,
    expected_groups: Tuple,
) -> Tuple:
    groups = (
        (
            np.arange(894),
            np.arange(86),
            np.arange(854),
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
            dist_alerts.alert_date,
            dist_alerts.confidence,
        ),
        groups,
    )


def _compute(reduce_mask: xr.DataArray, reduce_groupbys: Tuple, expected_groups: Tuple):
    return xarray_reduce(
        reduce_mask,
        *reduce_groupbys,
        func="count",
        expected_groups=expected_groups,
        reindex=ReindexStrategy(
            blockwise=False, array_type=ReindexArrayType.SPARSE_COO
        ),
        fill_value=0,
    ).compute()


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
    saver(alerts_count_df, results_uri)
    return results_uri


def pipe(value, *functions):
    return reduce(lambda v, f: f(v), functions, value)
