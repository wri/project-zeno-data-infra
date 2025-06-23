import logging

import coiled
import numpy as np
import pandas as pd
import xarray as xr

from flox import ReindexArrayType, ReindexStrategy
from flox.xarray import xarray_reduce
from prefect import flow, task
from prefect.logging import get_run_logger

from .dist.create_zarr import (
    create_zarr as create_zarr_func,
    get_tiles as get_tiles_func,
)
from .dist.check_for_new_alerts import get_latest_version
from .dist.gadm_dist_alerts_by_natural_lands import gadm_dist_alerts_by_natural_lands

DATA_LAKE_BUCKET = "gfw-data-lake"

logging.getLogger("distributed.client").setLevel(logging.ERROR)


@task
def get_new_dist_version() -> str:
    return get_latest_version("umd_glad_dist_alerts")


@task
def get_tiles(dist_version):
    return get_tiles_func("umd_glad_dist_alerts", dist_version)


@task
def create_cluster():
    cluster = coiled.Cluster(
        name="dist_alerts_zonal_stat_count",
        region="us-east-1",
        n_workers=10,
        tags={"project": "dist_alerts_zonal_stat"},
        scheduler_vm_types="r7g.xlarge",
        worker_vm_types="r7g.2xlarge",
        compute_purchase_option="spot_with_fallback",
    )
    cluster.adapt(minimum=10, maximum=50)
    client = cluster.get_client()

    return client


@task
def create_zarr(dist_version: str, tile_uris, overwrite=False) -> str:
    zarr_uri = create_zarr_func(dist_version, tile_uris, overwrite=overwrite)
    return zarr_uri


@task
def analyze_gadm_dist(dist_zarr_uri, version):
    results_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_dist_alerts/{version}/raster/epsg-4326/zonal_stats/dist_alerts_by_adm2_raw_test.parquet"
    logging.getLogger("distributed.client").setLevel(logging.ERROR)

    dist_alerts = xr.open_zarr(dist_zarr_uri)
    countries_from_clipped = xr.open_zarr(
        "s3://gfw-data-lake/gadm_administrative_boundaries/v4.1.85/raster/epsg-4326/zarr/adm0_clipped_to_dist.zarr"
    ).band_data

    regions_from_clipped = xr.open_zarr(
        "s3://gfw-data-lake/gadm_administrative_boundaries/v4.1.85/raster/epsg-4326/zarr/adm1_clipped_to_dist.zarr"
    ).band_data

    subregions_from_clipped = xr.open_zarr(
        "s3://gfw-data-lake/gadm_administrative_boundaries/v4.1.85/raster/epsg-4326/zarr/adm2_clipped_to_dist.zarr"
    ).band_data

    countries_from_clipped.name = "countries"
    regions_from_clipped.name = "regions"
    subregions_from_clipped.name = "subregions"
    alerts_count = xarray_reduce(
        dist_alerts.confidence,
        *(
            countries_from_clipped,
            regions_from_clipped,
            subregions_from_clipped,
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


@task
def run_validation_suite():
    pass


@flow(name="DIST alerts count")
def main(overwrite=False) -> str:
    dask_client = None
    logger = get_run_logger()
    try:
        dist_version = get_new_dist_version()
        tile_uris = get_tiles(dist_version)
        dask_client = create_cluster()
        dist_zarr_uri = create_zarr(dist_version, tile_uris, overwrite=overwrite)
        #result = analyze_gadm_dist(dist_zarr_uri, dist_version)
        result = gadm_dist_alerts_by_natural_lands(dist_zarr_uri, dist_version)
    except Exception:
        logger.error("DIST alerts analysis failed.")
        raise
    finally:
        if dask_client:
            dask_client.shutdown()

    return result


if __name__ == "__main__":
    main(overwrite=False)
