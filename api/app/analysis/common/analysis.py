import json
import logging
import os

import httpx
import xarray as xr
from shapely.geometry import shape

JULIAN_DATE_2021 = 2459215


async def send_request_to_data_api(url, params):
    params["x-api-key"] = _get_api_key()
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(url, params=params)
    return response.json()


async def get_geojson_from_data_api(aoi, send_request=send_request_to_data_api):
    url, params = get_geojson_request_for_data_api(aoi)
    response = await send_request(url, params)

    if "data" not in response:
        logging.error(
            f"Unable to get GeoJSON from Data API for AOI {aoi}, Data API returned: \n{response}"
        )
        raise ValueError("Unable to get GeoJSON from Data API.")

    geojson = json.loads(response["data"][0]["gfw_geojson"])
    return geojson


def get_geojson_request_for_data_api(aoi):
    if aoi["type"] == "key_biodiversity_area":
        url = "https://data-api.globalforestwatch.org/dataset/birdlife_key_biodiversity_areas/latest/query"
        sql = f"select gfw_geojson from data where sitrecid = {aoi['id']}"
    elif aoi["type"] == "protected_area":
        url = "https://data-api.globalforestwatch.org/dataset/wdpa_protected_areas/latest/query"
        sql = f"select gfw_geojson from data where wdpaid = {aoi['id']}"
    elif aoi["type"] == "indigenous_land":
        url = (
            "https://data-api.globalforestwatch.org/dataset/landmark_icls/latest/query"
        )
        sql = f"select gfw_geojson from data where objectid = {aoi['id']}"
    else:
        raise ValueError(f"Unable to retrieve AOI type {aoi['type']} from Data API.")
    return url, {"sql": sql}


async def get_geojson(aoi, geojson_from_predfined_aoi=get_geojson_from_data_api):
    if aoi["type"] == "geojson":
        geojson = aoi["geojson"]
    else:
        geojson = await geojson_from_predfined_aoi(aoi)
    return geojson


def clip_xarr_to_geojson(xarr, geojson):
    geom = shape(geojson)
    sliced = xarr.sel(
        x=slice(geom.bounds[0], geom.bounds[2]),
        y=slice(geom.bounds[3], geom.bounds[1]),
    ).squeeze("band")
    clipped = sliced.rio.clip([geojson])
    return clipped


def read_zarr_clipped_to_geojson(uri, geojson):
    zarr = read_zarr(uri)
    clipped = clip_xarr_to_geojson(zarr, geojson)
    return clipped


def read_zarr(uri):
    return xr.open_zarr(
        uri,
        storage_options={"requester_pays": True},
    )


def _get_api_key():
    return os.environ["API_KEY"]
