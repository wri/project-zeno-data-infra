import requests
import json
import os
from shapely.geometry import shape
from flox.xarray import xarray_reduce
import pandas as pd
import xarray as xr
import numpy as np


JULIAN_DATE_2021 = 2459215
NATURAL_LANDS_CLASSES = {
    2: "Forest",
    3: "Short vegetation",
    4: "Water",
    5: "Mangroves",
    6: "Bare",
    7: "Snow/Ice",
    8: "Wetland forest",
    9: "Peat forest",
    10: "Wetland short vegetation",
    11: "Peat short vegetation",
    12: "Cropland",
    13: "Built-up",
    14: "Tree cover",
    15: "Short vegetation",
    16: "Water",
    17: "Wetland tree cover",
    18: "Peat tree cover",
    19: "Wetland short vegetation",
    20: "Peat short vegetation",
    21: "Bare"
}
DIST_DRIVERS = {
    1: "Wildfire",
    2: "Flooding",
    3: "Crop management",
    4: "Potential conversion",
    5: "Unclassified",
}


async def send_request_to_data_api(url):
    api_key = os.environ["API_KEY"]
    url += f"&x-api-key={api_key}"
    return requests.get(url).json()


async def get_geojson_from_data_api(aoi, send_request=send_request_to_data_api):
    url = get_geojson_url_for_data_api(aoi)
    response = await send_request(url)

    if "data" not in response:
        raise ValueError("Unable to get GeoJSON from Data API.")
    
    geojson = json.loads(response["data"][0]["gfw_geojson"])
    return geojson


def get_geojson_url_for_data_api(aoi):
    if aoi["type"] == "key_biodiversity_area":
        url = f"https://data-api.globalforestwatch.org/dataset/birdlife_key_biodiversity_areas/latest/query?sql=select gfw_geojson from data where sitrecid = {aoi['id']}"
    elif aoi["type"] == "protected_area":
        url = f"https://data-api.globalforestwatch.org/dataset/wdpa_protected_areas/latest/query?sql=select gfw_geojson from data where wdpaid = {aoi['id']}"
    elif aoi["type"] == "indigenous_land":
        url = f"https://data-api.globalforestwatch.org/dataset/landmark_icls/latest/query?sql=select gfw_geojson from data where objectid = {aoi['id']}"

    return url


async def get_geojson(aoi, geojson_from_predfined_aoi=get_geojson_from_data_api):
    if aoi["type"] == "geojson":
        geojson = aoi["geojson"]
    else:
        geojson = await geojson_from_predfined_aoi(aoi)
    return geojson



async def zonal_statistics(geojson, aoi, intersection=None):
    dist_obj_name = "s3://gfw-data-lake/umd_glad_dist_alerts/v20250510/raster/epsg-4326/zarr/date_conf.zarr"
    dist_alerts = clip_xarr_to_geojson(xr.open_zarr(dist_obj_name), geojson)

    groupby_layers = [dist_alerts.alert_date, dist_alerts.confidence]
    expected_groups = [np.arange(731, 1590), [1, 2, 3]]
    if intersection == 'natural_lands':
        natural_lands = clip_xarr_to_geojson(xr.open_zarr(
            's3://gfw-data-lake/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes_clipped_to_dist.zarr'
        ).band_data, geojson)
        natural_lands.name = "natural_land_class"

        groupby_layers.append(natural_lands)
        expected_groups.append(np.arange(22))
    elif intersection == 'driver':
        dist_drivers = clip_xarr_to_geojson(xr.open_zarr(
            "s3://gfw-data-lake/umd_glad_dist_alerts_driver/zarr/umd_dist_alerts_drivers.zarr"
        ).band_data, geojson)
        dist_drivers.name = "ldacs_driver"

        groupby_layers.append(dist_drivers) 
        expected_groups.append(np.arange(5))

    alerts_count = xarray_reduce(
        dist_alerts.alert_date, 
        *tuple(groupby_layers),
        func='count',
        expected_groups=tuple(expected_groups),
    ).compute()
    alerts_count.name = 'value'

    alerts_df = alerts_count.to_dataframe().drop("band", axis=1).drop("spatial_ref", axis=1).reset_index()
    alerts_df.confidence = alerts_df.confidence.map({2: 'low', 3: 'high'})
    alerts_df.alert_date = pd.to_datetime(alerts_df.alert_date + JULIAN_DATE_2021, origin='julian', unit='D').dt.strftime('%Y-%m-%d')
    
    if "id" in aoi:
        alerts_df[aoi["type"]] = aoi["id"]
    
    
    if intersection == 'natural_lands':
        alerts_df.natural_land_class = alerts_df.natural_land_class.apply(lambda x: NATURAL_LANDS_CLASSES.get(x, 'Unclassified'))
    elif intersection == 'driver':
        alerts_df.ldacs_driver = alerts_df.ldacs_driver.apply(lambda x: DIST_DRIVERS.get(x, 'Unclassified'))

    alerts_df = alerts_df[alerts_df.value > 0]
    return alerts_df


def clip_xarr_to_geojson(xarr, geojson):
    geom = shape(geojson)
    sliced = xarr.sel(x=slice(geom.bounds[0],geom.bounds[2]), y=slice(geom.bounds[3],geom.bounds[1]),).squeeze("band")
    clipped = sliced.rio.clip([geojson])
    return clipped