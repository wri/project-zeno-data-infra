from typing import Callable, Tuple, Optional
import pandas as pd
import xarray as xr
from flox import ReindexArrayType, ReindexStrategy
from flox.xarray import xarray_reduce

from pipelines.globals import (
    country_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)


def load_data(
    dist_zarr_uri: str,
    contextual_uri: Optional[str] = None,
) -> Tuple[xr.Dataset, ...]:
    """Load in the Dist alert Zarr, the GADM zarrs, and possibly a contextual layer zarr"""

    dist_alerts = _load_zarr(dist_zarr_uri)

    country = _load_zarr(country_zarr_uri)
    country_aligned = xr.align(dist_alerts, country, join="left")[1]
    region = _load_zarr(region_zarr_uri)
    region_aligned = xr.align(dist_alerts, region, join="left")[1]
    subregion = _load_zarr(subregion_zarr_uri)
    subregion_aligned = xr.align(dist_alerts, subregion, join="left")[1]

    if contextual_uri is not None:
        contextual_layer = _load_zarr(contextual_uri)
        contextual_layer_aligned = xr.align(dist_alerts, contextual_layer, join="left")[
            1
        ]
    else:
        contextual_layer_aligned = None

    return (
        dist_alerts,
        country_aligned,
        region_aligned,
        subregion_aligned,
        contextual_layer_aligned,
    )

def compute(reduce_mask: xr.DataArray, reduce_groupbys: Tuple, expected_groups: Tuple, funcname: str):
    print("Starting reduce")
    alerts_count = xarray_reduce(
        reduce_mask,
        *reduce_groupbys,
        func=funcname,
        expected_groups=expected_groups,
        reindex=ReindexStrategy(
            blockwise=False, array_type=ReindexArrayType.SPARSE_COO
        ),
        fill_value=0,
    ).compute()
    print("Finished reduce")
    return alerts_count


def create_result_dataframe(alerts_count: xr.Dataset) -> pd.DataFrame:
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


def save_results(
    df: pd.DataFrame, results_uri: str
) -> str:
    print("Starting parquet")

    _save_parquet(df, results_uri)
    print("Finished parquet")
    return results_uri


# _load_zarr and _save_parquet are the functions being mocked by the unit tests.
def _save_parquet(df: pd.DataFrame, results_uri: str) -> None:
    df.to_parquet(results_uri, index=False)


def _load_zarr(zarr_uri):
    return xr.open_zarr(zarr_uri)
