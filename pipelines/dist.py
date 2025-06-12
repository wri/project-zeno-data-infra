import itertools
import logging
from typing import Optional
from prefect import flow, task
import coiled

import numpy as np
import requests
import xarray as xr
import fsspec
import pandas as pd
from flox.xarray import xarray_reduce
import numpy as np
from flox import ReindexArrayType, ReindexStrategy
import fsspec
import numpy as np
import xarray as xr
import fsspec
import pandas as pd
from flox.xarray import xarray_reduce
import numpy as np
import boto3


CONTEXTUAL_LAYERS = [None, "wri_natural_lands"] #, "ldacs", "umd_land_cover"]
AOIs = ["gadm_administrative_boundaries"] #, "birdlife_key_biodiversity_areas", "wdpa_protected_areas", "landmark_indigenous_areas"]

def get_uri(feature):
    raw = feature['properties']['name'].split('/')[2:]
    uri = '/'.join(['s3:/'] + raw)
    return uri

@task
def get_new_dist_version() -> str:
    latest = requests.get("https://data-api.globalforestwatch.org/dataset/umd_glad_dist_alerts/latest").json()
    # todo, check if we've already created this
    return latest["data"]["version"]

@task
def create_zarr(dist_version: str) -> str:
    zarr_uri = f"s3://gfw-data-lake/umd_glad_dist_alerts/{dist_version}/raster/epsg-4326/zarr/date_conf.zarr"
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket="gfw-data-lake", Key=f"umd_glad_dist_alerts/{dist_version}/raster/epsg-4326/zarr/date_conf.zarr/zarr.json")
        return zarr_uri
    except:
        pass
    
    cluster = coiled.Cluster(
        name="dist_alerts_zonal_stat_count",
        region="us-east-1",
        n_workers=20,
        tags={"project": "dist_alerts_zonal_stat"},
        scheduler_vm_types="r7g.xlarge",
        worker_vm_types="r7g.2xlarge",
        compute_purchase_option="spot_with_fallback"
    )
    client = cluster.get_client()
    logging.getLogger("distributed.client").setLevel(logging.ERROR) 

    dist_alerts_tiles = pd.read_json(
        
    )
    dist_alerts_tile_uris = dist_alerts_tiles.features.apply(get_uri)
    dist_alerts = xr.open_mfdataset(
        dist_alerts_tile_uris,
        parallel=True,
        chunks={'x': 10000, 'y':10000}
    ).astype(np.int16)

    alert_date = dist_alerts.band_data % 10000
    alert_conf = (dist_alerts.band_data // 10000).astype(np.uint8)
    alert_conf.name = "confidence"
    alert_date.name = "alert_date"

    date_conf = xr.merge((alert_conf, alert_date))
    date_conf.to_zarr(zarr_uri, mode="w")

    client.close()
    cluster.close()

    return zarr_uri

@task
def analyze_gadm_dist(dist_zarr_uri):
    cluster = coiled.Cluster(
        name="dist_alerts_zonal_stat_count",
        region="us-east-1",
        n_workers=50,
        tags={"project": "dist_alerts_zonal_stat"},
        scheduler_vm_types="r7g.xlarge",
        worker_vm_types="r7g.2xlarge",
        compute_purchase_option="spot_with_fallback"
    )
    client = cluster.get_client()
    logging.getLogger("distributed.client").setLevel(logging.ERROR) 

    dist_alerts = xr.open_zarr(dist_zarr_uri)
    countries_from_clipped = xr.open_zarr(
        's3://gfw-data-lake/gadm_administrative_boundaries/v4.1.85/raster/epsg-4326/zarr/adm0_clipped_to_dist.zarr'
    ).band_data
    
    regions_from_clipped = xr.open_zarr(
        's3://gfw-data-lake/gadm_administrative_boundaries/v4.1.85/raster/epsg-4326/zarr/adm1_clipped_to_dist.zarr'
    ).band_data
    
    subregions_from_clipped = xr.open_zarr(
        's3://gfw-data-lake/gadm_administrative_boundaries/v4.1.85/raster/epsg-4326/zarr/adm2_clipped_to_dist.zarr'
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
            dist_alerts.confidence
        ),
        func='count',
        expected_groups=(
            np.arange(894),
            np.arange(86),
            np.arange(854),
            np.arange(731, 1590),
            [1, 2, 3]
        ),
        reindex=ReindexStrategy(
            blockwise=False, array_type=ReindexArrayType.SPARSE_COO
        ),
        fill_value=0
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
    df.to_parquet('dist_alerts_by_adm2_raw.parquet', index=False)

    client.close()
    cluster.close()

@task
def run_validation_suite():
    pass

@flow
def main() -> list[str]:
    dist_version = get_new_dist_version()
    if not dist_version:
        return []

    dist_zarr_uri = create_zarr(dist_version)

    #results = analyze_dist.map(itertools.product(AOIs, CONTEXTUAL_LAYERS), dist_zarr_uri)
    result = analyze_gadm_dist(dist_zarr_uri)

    return [result]


if __name__ == "__main__":
    main()