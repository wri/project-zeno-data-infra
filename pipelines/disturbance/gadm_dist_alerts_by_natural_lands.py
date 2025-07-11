import numpy as np
import xarray as xr
import pandas as pd
import logging
from flox.xarray import xarray_reduce
from flox import ReindexArrayType, ReindexStrategy

from .check_for_new_alerts import s3_object_exists
from ..globals import DATA_LAKE_BUCKET, country_zarr_uri, region_zarr_uri, subregion_zarr_uri

def gadm_dist_alerts_by_natural_lands(zarr_uri: str, version: str, overwrite: bool) -> str:
    """Run DIST alerts analysis by natural lands using Dask to create parquet, upload to S3 and return URI."""

    results_key = f"umd_glad_dist_alerts/{version}/tabular/epsg-4326/zonal_stats/dist_alerts_by_adm2_natural_lands.parquet"
    results_uri = f"s3://{DATA_LAKE_BUCKET}/{results_key}"

    if not overwrite and s3_object_exists(DATA_LAKE_BUCKET, results_key):
        return results_uri

    logging.getLogger("distributed.client").setLevel(logging.ERROR)

    dist_alerts = xr.open_zarr(zarr_uri)
    country = xr.open_zarr(country_zarr_uri)
    country_from_clipped = xr.align(dist_alerts, country, join='left')[1].band_data
    region = xr.open_zarr(region_zarr_uri)
    region_from_clipped = xr.align(dist_alerts, region, join='left')[1].band_data
    subregion = xr.open_zarr(subregion_zarr_uri)
    subregion_from_clipped = xr.align(dist_alerts, subregion, join='left')[1].band_data

    natural_lands = xr.open_zarr(
        f's3://{DATA_LAKE_BUCKET}/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes.zarr'
    )
    natural_lands_from_clipped = xr.align(dist_alerts, natural_lands, join='left')[1].band_data

    adm0_ids = [
        0, 4, 8, 10, 12, 16, 20, 24, 28, 31, 32, 36, 40, 44, 48, 50, 51, 52, 56, 60,
        64, 68, 70, 72, 74, 76, 84, 86, 90, 92, 96, 100, 104, 108, 112, 116, 120,
        124, 132, 136, 140, 144, 148, 152, 156, 158, 162, 166, 170, 174, 175, 178,
        180, 184, 188, 191, 192, 196, 203, 204, 208, 212, 214, 218, 222, 226, 231,
        232, 233, 234, 238, 239, 242, 246, 248, 250, 254, 258, 260, 262, 266, 268,
        270, 275, 276, 288, 292, 296, 300, 304, 308, 312, 316, 320, 324, 328, 332,
        334, 336, 340, 344, 348, 352, 356, 360, 364, 368, 372, 376, 380, 384, 388,
        392, 398, 400, 404, 408, 410, 414, 417, 418, 422, 426, 428, 430, 434, 438,
        440, 442, 446, 450, 454, 458, 462, 466, 470, 474, 478, 480, 484, 492, 496,
        498, 499, 500, 504, 508, 512, 516, 520, 524, 528, 531, 533, 534, 535, 540,
        548, 554, 558, 562, 566, 570, 574, 578, 580, 581, 583, 584, 585, 586, 591,
        598, 600, 604, 608, 612, 616, 620, 624, 626, 630, 634, 638, 642, 643, 646,
        652, 654, 659, 660, 662, 663, 666, 670, 674, 678, 682, 686, 688, 690, 694,
        702, 703, 704, 705, 706, 710, 716, 724, 728, 729, 732, 740, 744, 748, 752,
        756, 760, 762, 764, 768, 772, 776, 780, 784, 788, 792, 795, 796, 798, 800,
        804, 807, 818, 826, 831, 832, 833, 834, 840, 850, 854, 858, 860, 862, 876,
        882, 887, 894
    ]

    alert_dates = np.arange(731, 1590)  # we should get this from metadata which includes `content_date_range`

    country_from_clipped.name = "countries"
    region_from_clipped.name = "regions"
    subregion_from_clipped.name = "subregions"
    natural_lands_from_clipped.name = "natural_lands"
    print("Starting reduce")
    alerts_count = xarray_reduce(
        dist_alerts.confidence,
        *(
            country_from_clipped,
            region_from_clipped,
            subregion_from_clipped,
            natural_lands_from_clipped,
            dist_alerts.alert_date,
            dist_alerts.confidence
        ),
        func='count',
        expected_groups=(
            adm0_ids,
            np.arange(86),
            np.arange(854),
            np.arange(22),
            alert_dates,
            [1, 2, 3]
        ),
        reindex=ReindexStrategy(
            blockwise=False, array_type=ReindexArrayType.SPARSE_COO
        ),
        fill_value=0
    ).compute()
    print("Finished reduce")

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
    print("Starting parquet")
    df.to_parquet(results_uri, index=False)
    print("Finished parquet")
    return results_uri
