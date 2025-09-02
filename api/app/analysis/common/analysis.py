import json
import logging
import os
from typing import Iterable

import duckdb
import httpx
import xarray as xr
from shapely.geometry import shape

JULIAN_DATE_2021 = 2459215


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
        sql = (
            f"select gfw_geojson from data where wdpaid in {value_list} order by wdpaid"
        )
    elif aoi["type"] == "indigenous_land":
        url = (
            "https://data-api.globalforestwatch.org/dataset/landmark_icls/latest/query"
        )
        sql = f"select gfw_geojson from data where objectid in {value_list} order by objectid"
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

    # Exit early if the geometry is full out of bounds of the dataset
    # Not generally right, since requires data variable to be "band_data"
    # Will take Justin's better fix.
    if sliced.band_data.size == 0:
        return sliced

    clipped = sliced.rio.clip([geojson])
    return clipped


def read_zarr_clipped_to_geojson(uri, geojson):
    zarr = read_zarr(uri)
    zarr.rio.write_crs("EPSG:4326", inplace=True)
    clipped = clip_zarr_to_geojson(zarr, geojson)
    return clipped


def read_zarr(uri):
    return xr.open_zarr(
        uri,
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
