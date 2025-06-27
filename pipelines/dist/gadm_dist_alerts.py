import logging

import numpy as np
import pandas as pd
import xarray as xr
from flox import ReindexArrayType, ReindexStrategy
from flox.xarray import xarray_reduce


DATA_LAKE_BUCKET = "gfw-data-lake"
GADM_VERSION = "v4.1.85"

country_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{GADM_VERSION}/raster/epsg-4326/zarr/adm0_clipped_to_dist.zarr"
region_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{GADM_VERSION}/raster/epsg-4326/zarr/adm1_clipped_to_dist.zarr"
subregion_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{GADM_VERSION}/raster/epsg-4326/zarr/adm2_clipped_to_dist.zarr"


def gadm_dist_alerts(dist_zarr_uri: str, dist_version: str):
    """Count DIST alerts by GADM boundary, confidence, and date, and export grouped results to a Parquet file in S3."""
    results_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_dist_alerts/{dist_version}/tabular/epsg-4326/zonal_stats/dist_alerts_by_adm2_raw_test.parquet"

    logging.getLogger("distributed.client").setLevel(logging.ERROR)

    dist_alerts = xr.open_zarr(dist_zarr_uri)

    country_from_clipped = xr.open_zarr(country_zarr_uri).band_data
    region_from_clipped = xr.open_zarr(region_zarr_uri).band_data
    subregion_from_clipped = xr.open_zarr(subregion_zarr_uri).band_data

    country_from_clipped.name = "country"
    region_from_clipped.name = "region"
    subregion_from_clipped.name = "subregion"

    alerts_count = xarray_reduce(
        dist_alerts.confidence,
        *(
            country_from_clipped,
            region_from_clipped,
            subregion_from_clipped,
            dist_alerts.alert_date,
            dist_alerts.confidence,
        ),
        func="count",
        expected_groups=(
            np.arange(894),
            np.arange(86),
            np.arange(854),
            np.arange(731, 1590),
            [1, 2, 3],
        ),
        reindex=ReindexStrategy(
            blockwise=False, array_type=ReindexArrayType.SPARSE_COO
        ),
        fill_value=0,
    ).compute()

    sparse_data = alerts_count.data

    dim_names = alerts_count.dims
    indices = sparse_data.coords
    values = sparse_data.data

    coord_dict = {
        dim: alerts_count.coords[dim].values[indices[i]]
        for i, dim in enumerate(dim_names)
    }
    coord_dict["value"] = values

    df = pd.DataFrame(coord_dict)
    df.to_parquet(results_uri, index=False)

    return results_uri
