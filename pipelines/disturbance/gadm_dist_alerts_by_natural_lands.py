import logging
from typing import Optional
import numpy as np

from .check_for_new_alerts import s3_object_exists
from ..globals import DATA_LAKE_BUCKET

from .stages import (
    LoaderType,
    ExpectedGroupsType,
    SaverType,
    _s3_loader,
    _parquet_saver,
    _setup,
    _compute,
    _create_data_frame,
    _save_results,
    pipe,
)


def gadm_dist_alerts_by_natural_lands(
    dist_zarr_uri: str,
    dist_version: str,
    loader: LoaderType = _s3_loader,
    groups: Optional[ExpectedGroupsType] = None,
    saver: SaverType = _parquet_saver,
    overwrite: bool = False,
) -> str:
    """Run DIST alerts analysis by natural lands using Dask to create parquet, upload to S3 and return URI."""
    logging.getLogger("distributed.client").setLevel(logging.ERROR)

    contextual_uri = f"s3://{DATA_LAKE_BUCKET}/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes.zarr"

    expected_groups = (
        (
            np.arange(894),  # country ISO codes
            np.arange(86),  # region codes
            np.arange(854),  # subregion codes
            np.arange(22),  # natural lands categories
            np.arange(731, 1590),  # dates values
            [1, 2, 3],  # confidence values
        )
        if groups is None
        else groups
    )

    contextual_column_name = "natural_lands"

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
