"""Shared, dataset-agnostic helpers for Land GHG inventory stages."""

from typing import Optional

import xarray as xr
from shapely.geometry import Polygon

from pipelines.prefect_flows.common_stages import _load_zarr


def align_to(reference: xr.Dataset, uri: str) -> xr.DataArray:
    """Load a contextual band_data zarr and snap it to the reference grid."""
    layer = _load_zarr(uri).band_data
    if "band" in layer.dims:
        layer = layer.isel(band=0, drop=True)
    layer = layer.reindex_like(reference, method="nearest", tolerance=1e-4)
    return xr.align(reference, layer, join="left")[1]


def clip(dataset: xr.Dataset, bbox: Optional[Polygon]) -> xr.Dataset:
    if bbox is None:
        return dataset
    min_x, min_y, max_x, max_y = bbox.bounds
    return dataset.sel(x=slice(min_x, max_x), y=slice(max_y, min_y))
