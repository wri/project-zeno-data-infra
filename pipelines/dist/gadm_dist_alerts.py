import logging

import numpy as np
import pandas as pd
import xarray as xr
from flox import ReindexArrayType, ReindexStrategy
from flox.xarray import xarray_reduce


DATA_LAKE_BUCKET = "gfw-data-lake"


def gadm_dist_alerts(dist_zarr_uri: str, dist_version: str):
    """Count DIST alerts by GADM boundary, confidence, and date, and export grouped results to a Parquet file in S3."""
    logging.getLogger("distributed.client").setLevel(logging.ERROR)
    reduce_mask, reduce_groupbys, expected_groups = _load_data(dist_zarr_uri)
    alerts_count = _compute(reduce_mask, reduce_groupbys, expected_groups)
    alerts_count_df = _create_data_frame(alerts_count)
    return _save_results(alerts_count_df, dist_version)


def _load_data(dist_zarr_uri):
    gadm_version = "v4.1.85"
    country_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{gadm_version}/raster/epsg-4326/zarr/adm0_clipped_to_dist.zarr"
    region_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{gadm_version}/raster/epsg-4326/zarr/adm1_clipped_to_dist.zarr"
    subregion_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{gadm_version}/raster/epsg-4326/zarr/adm2_clipped_to_dist.zarr"

    dist_alerts = xr.open_zarr(dist_zarr_uri)
    country = xr.open_zarr(country_zarr_uri).band_data
    region = xr.open_zarr(region_zarr_uri).band_data
    subregion = xr.open_zarr(subregion_zarr_uri).band_data

    country.name = "country"
    region.name = "region"
    subregion.name = "subregion"

    return (
        dist_alerts.confidence,
        (
            country,
            region,
            subregion,
            dist_alerts.alert_date,
            dist_alerts.confidence,
        ),
        (
            np.arange(894),
            np.arange(86),
            np.arange(854),
            np.arange(731, 1590),
            [1, 2, 3],
        )
    )


def _compute(reduce_mask, reduce_groupbys, expected_groups):
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


def _create_data_frame(alerts_count):
    sparse_data = alerts_count.data

    dim_names = alerts_count.dims
    indices = sparse_data.coords
    values = sparse_data.data

    coord_dict = {
        dim: alerts_count.coords[dim].values[indices[i]]
        for i, dim in enumerate(dim_names)
    }
    coord_dict["value"] = values

    return pd.DataFrame(coord_dict)


def _save_results(alerts_count_df, dist_version):
    results_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_dist_alerts/{dist_version}/tabular/epsg-4326/zonal_stats/dist_alerts_by_adm2_raw_test.parquet"
    alerts_count_df.to_parquet(results_uri, index=False)
    return results_uri