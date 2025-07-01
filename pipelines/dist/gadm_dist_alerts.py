import logging
from typing import Callable, Tuple
import numpy as np
import pandas as pd
import xarray as xr
from flox import ReindexArrayType, ReindexStrategy
from flox.xarray import xarray_reduce

DATA_LAKE_BUCKET = "gfw-data-lake"

LoaderType = Callable[[str], Tuple[xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]

def _default_loader(dist_zarr_uri: str) -> Tuple[xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset]:
    gadm_version = "v4.1.85"
    country_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{gadm_version}/raster/epsg-4326/zarr/adm0_clipped_to_dist.zarr"
    region_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{gadm_version}/raster/epsg-4326/zarr/adm1_clipped_to_dist.zarr"
    subregion_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{gadm_version}/raster/epsg-4326/zarr/adm2_clipped_to_dist.zarr"

    return (
        xr.open_zarr(dist_zarr_uri),
        xr.open_zarr(country_zarr_uri),
        xr.open_zarr(region_zarr_uri),
        xr.open_zarr(subregion_zarr_uri),
    )


def _default_saver(alerts_count_df: pd.DataFrame, results_uri: str) -> None:
    alerts_count_df.to_parquet(results_uri, index=False)


def gadm_dist_alerts(
        dist_zarr_uri: str,
        dist_version: str,
        loader: LoaderType = _default_loader,
        groups: ExpectedGroupsType = None,
        saver: SaverType = _default_saver,
) -> str:
    """Count DIST alerts by GADM boundary, confidence, and date, and export grouped results to a Parquet file in S3."""
    logging.getLogger("distributed.client").setLevel(logging.ERROR)
    dist_alerts, country, region, subregion = loader(dist_zarr_uri)
    reduce_mask, reduce_groupbys, expected_groups = _processor(dist_alerts, country, region, subregion, groups)
    alerts_count = _compute(reduce_mask, reduce_groupbys, expected_groups)
    alerts_count_df = _create_data_frame(alerts_count)
    return _save_results(alerts_count_df, dist_version, saver)


def _processor(
        dist_alerts: xr.Dataset,
        country: xr.Dataset,
        region: xr.Dataset,
        subregion: xr.Dataset,
        expected_groups: Tuple,
) -> Tuple:
    groups = (
        np.arange(894),
        np.arange(86),
        np.arange(854),
        np.arange(731, 1590),
        [1, 2, 3],
    ) if expected_groups is None else expected_groups

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


def _save_results(alerts_count_df: pd.DataFrame, dist_version: str, saver: Callable) -> str:
    results_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_dist_alerts/{dist_version}/tabular/epsg-4326/zonal_stats/dist_alerts_by_adm2_raw_test.parquet"
    saver(alerts_count_df, results_uri)
    return results_uri
