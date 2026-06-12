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
        sql = f"select gfw_geojson from data where wdpa_pid in {value_list} order by wdpa_pid"
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


def _infer_grid(coord: xr.DataArray) -> tuple[float, float]:
    """Infer (pixel-edge origin, pixel step magnitude) from a 1-D coord.

    The stored coordinate values are pixel centers and carry ~1e-11 of
    floating-point jitter accumulated across the global axis. Using the
    first two values gives us a step accurate to well within half a pixel,
    which is all we need for snapping slice bounds to pixel edges.
    """
    values = coord.values
    step = abs(float(values[1]) - float(values[0]))
    # Edge origin sits half a pixel "before" the first pixel center.
    # Direction-agnostic: works for ascending x and descending y.
    first = float(values[0])
    sign = 1.0 if values[-1] > values[0] else -1.0
    origin = first - sign * step / 2.0
    return origin, step


def _snap_to_grid(
    lo: float, hi: float, origin: float, step: float
) -> tuple[float, float]:
    """Snap [lo, hi] outward to the nearest pixel edges of the given grid.

    The snapped bounds land on pixel boundaries — halfway between stored
    pixel centers — so two zarrs on the same nominal grid but with slightly
    different floating-point coordinate values will deterministically
    select the same set of pixels.
    """
    lo_snapped = np.floor((lo - origin) / step) * step + origin
    hi_snapped = np.ceil((hi - origin) / step) * step + origin
    return float(lo_snapped), float(hi_snapped)


def clip_zarr_to_geojson(xarr: xr.Dataset, geojson):
    geom = shape(geojson)
    minx, miny, maxx, maxy = geom.bounds

    # Snap the slice bounds to the dataset's nominal pixel grid before sel.
    # Two zarrs that nominally share a grid may have x/y coord arrays that
    # differ by ~1e-11 due to stored float64 jitter; without snapping, a
    # geometry bound that lands within that jitter of a pixel center can
    # cause sel to include the pixel in one zarr and exclude it in the
    # other, producing off-by-one mismatches that break downstream
    # alignment (e.g. flox.xarray_reduce with join="exact").
    x_origin, x_step = _infer_grid(xarr.x)
    y_origin, y_step = _infer_grid(xarr.y)
    minx, maxx = _snap_to_grid(minx, maxx, x_origin, x_step)
    miny, maxy = _snap_to_grid(miny, maxy, y_origin, y_step)

    sliced: xr.Dataset = xarr.sel(
        x=slice(minx, maxx),
        y=slice(maxy, miny),
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
