# flake8: noqa: E501
import json
import logging
import os
from enum import Enum
from typing import Iterable

import duckdb
import httpx
import numpy as np
import xarray as xr
from rioxarray.exceptions import NoDataInBounds
from shapely.geometry import shape

JULIAN_DATE_2021 = 2459215

# SBTN natural lands class codes included by each land_filter value. To add a
# filter, add an entry here and its value to the land_filter Literal.
LAND_FILTER_CLASSES = {
    # natural forests, short vegetation, water, mangroves, bare, snow, and
    # wetland/peat forests and short vegetation
    "natural_lands": list(range(2, 12)),
    # natural forests, mangroves, wetland natural forests and natural peat forests
    "natural_forests": [2, 5, 8, 9],
}


class FeatureTooSmallError(Exception):
    """Raised when an AOI is too small to contain any pixel centroids."""

    pass


async def send_request_to_data_api(url, params):
    params["x-api-key"] = _get_api_key()
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(url, params=params)
    return response.json()


async def get_geojsons_from_data_api(aoi, send_request=send_request_to_data_api):
    url, params = get_geojson_request_for_data_api(aoi)
    response = await send_request(url, params)

    if "data" not in response:
        logging.error(
            f"Unable to get GeoJSON from Data API for AOI {aoi}, Data API returned: \n{response}"
        )
        raise ValueError("Unable to get GeoJSON from Data API.")

    geojsons = [json.loads(data["gfw_geojson"]) for data in response["data"]]
    return geojsons


def get_geojson_request_for_data_api(aoi):
    value_list = get_sql_in_list(aoi["ids"])
    if aoi["type"] == "key_biodiversity_area":
        url = "https://data-api.globalforestwatch.org/dataset/birdlife_key_biodiversity_areas/latest/query"
        sql = f"select gfw_geojson from data where sitrecid in {value_list} order by sitrecid"
    elif aoi["type"] == "protected_area":
        url = "https://data-api.globalforestwatch.org/dataset/wdpa_protected_areas/latest/query"
        sql = f"select gfw_geojson from data where site_pid in {value_list} order by site_pid"
    elif aoi["type"] == "indigenous_land":
        url = "https://data-api.globalforestwatch.org/dataset/landmark_ip_lc_and_indicative_poly/latest/query"
        sql = f"select gfw_geojson from data where landmark_id in {value_list} order by landmark_id"
    else:
        raise ValueError(f"Unable to retrieve AOI type {aoi['type']} from Data API.")
    return url, {"sql": sql}


async def get_geojson(aoi, geojsons_from_predefined_aoi=get_geojsons_from_data_api):
    if aoi["type"] == "feature_collection":
        geojson = aoi["feature_collection"]["features"]
    else:
        geojson = await geojsons_from_predefined_aoi(aoi)
    return geojson


def clip_zarr_to_geojson(xarr: xr.Dataset, geojson):
    geom = shape(geojson)

    sliced: xr.Dataset = xarr.sel(
        x=slice(geom.bounds[0], geom.bounds[2]),
        y=slice(geom.bounds[3], geom.bounds[1]),
    )
    if "band" in sliced.dims:
        sliced = sliced.squeeze("band")

    # Exit early if the geometry is fully out of bounds of the dataset, so all the
    # data variables are already empty. Will take Justin's better fix.
    if all(d.size == 0 for d in sliced.data_vars.values()):
        return sliced

    try:
        clipped = sliced.rio.clip([geojson])
    except NoDataInBounds:
        raise FeatureTooSmallError("AOI is too small. Please select a larger AOI ")
    return clipped


def read_zarr_clipped_to_geojson(uri, geojson, group: str | None = None):
    zarr = read_zarr(uri, group=group)
    if not zarr.dims:
        raise ValueError(
            f"Zarr at {uri} (group={group!r}) opened with no dimensions. "
            "The zarr may be a group container requiring a 'group' parameter, "
            "or the store structure may have changed."
        )
    zarr.rio.write_crs("EPSG:4326", inplace=True)
    clipped = clip_zarr_to_geojson(zarr, geojson)
    return clipped


def read_zarr(uri, group: str | None = None):
    return _open_zarr(uri, group=group)


def read_zarr_resampled_to_grid(uri, target) -> xr.DataArray:
    """Read a zarr's band_data resampled to the target's grid (e.g. a 30m layer
    onto the 10m alerts already clipped to an AOI).

    Unlike read_zarr_clipped_to_geojson, this doesn't mask by the AOI geometry,
    since the target is already clipped. Masking the coarser layer would drop its
    pixels whose centers are just outside the AOI, even though finer target
    pixels inside the AOI fall in them.
    """
    layer = read_zarr(uri).band_data
    if "band" in layer.dims:
        layer = layer.squeeze("band", drop=True)
    return resample_to_grid(layer, target)


def resample_to_grid(layer: xr.DataArray, target) -> xr.DataArray:
    """Nearest-neighbor resample a (coarser) layer to the target's grid.

    Each target pixel takes the value of the layer pixel its center falls in. The
    tolerance is half a layer pixel, since e.g. 10m pixel centers never coincide
    with 30m pixel centers, so a tiny tolerance like 1e-5 would match nothing.
    Target pixels outside the layer's extent are set to 0.

    Matching is done on the x/y coordinates in degrees, so both must be in
    EPSG:4326 (as all our zarrs are), and the layer's pixels must be square,
    since the tolerance is taken from its x step and applied to both axes.
    """
    check_square_pixels(layer, "layer")
    layer_resolution = abs(float(layer.x[1] - layer.x[0]))
    return layer.reindex_like(
        target, method="nearest", tolerance=layer_resolution / 2, fill_value=0
    ).astype(layer.dtype)


def check_square_pixels(grid, name: str) -> None:
    """Raise if grid's x and y pixel steps differ, since resampling takes its
    tolerance from the x step and applies it to both axes."""
    x_step = abs(float(grid.x[1] - grid.x[0]))
    y_step = abs(float(grid.y[1] - grid.y[0]))
    if not np.isclose(x_step, y_step, rtol=1e-6):
        raise ValueError(
            f"{name} must have square pixels, but its x step is {x_step} and "
            f"its y step is {y_step}"
        )


def to_land_filter_mask(
    natural_lands_classes: xr.DataArray, land_filter: str
) -> xr.DataArray:
    """Convert SBTN natural lands classes to 1 (one of the land_filter's classes in
    LAND_FILTER_CLASSES) or 0 (any other class, or no data)."""
    return natural_lands_classes.isin(LAND_FILTER_CLASSES[land_filter]).astype(np.uint8)


def _open_zarr(uri, group: str | None = None):
    return xr.open_zarr(
        uri,
        group=group,
        storage_options={"requester_pays": True},
    )


def _get_api_key():
    return os.environ["API_KEY"]


def get_sql_in_list(iter: Iterable) -> str:
    quoted = [f"'{item}'" for item in iter]
    joined = f"({', '.join(quoted)})"
    return joined


def initialize_duckdb():
    # Dumbly doing this per request since the STS token expires eventually otherwise
    # According to this issue, duckdb should auto refresh the token in 1.3.0,
    # but it doesn't seem to work for us and people are reporting the same on the issue
    # https://github.com/duckdb/duckdb-aws/issues/26
    # TODO do this on lifecycle start once autorefresh works
    duckdb.query(
        """
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER credential_chain,
            CHAIN 'instance;env;config'
        );
    """
    )


class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)
