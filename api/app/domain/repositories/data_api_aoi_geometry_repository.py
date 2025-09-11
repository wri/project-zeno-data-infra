import logging
import os
from typing import Iterable, List

import httpx
from shapely import Geometry, wkb
from shapely.geometry import shape


class DataApiAoiGeometryRepository:
    async def load(self, aoi_type: str, aoi_ids: List[str]) -> List[Geometry]:
        return await self._get_geojsons_from_data_api(aoi_type, aoi_ids)

    async def _get_geojsons_from_data_api(self, aoi_type, aoi_ids):
        url, params = self._get_geojson_request_for_data_api(aoi_type, aoi_ids)
        response = await self._send_request(url, params)

        if "data" not in response:
            logging.error(
                f"Unable to get GeoJSON from Data API for AOI {aoi_type}/{aoi_ids}, Data API returned: \n{response}"
            )
            raise ValueError("Unable to get GeoJSON from Data API.")

        geometries = [
            shape(wkb.loads(bytes.fromhex(data["geom"]))) for data in response["data"]
        ]
        return geometries

    def _get_geojson_request_for_data_api(self, aoi_type, aoi_ids):
        value_list = self._get_sql_in_list(aoi_ids)
        if aoi_type == "key_biodiversity_area":
            url = "https://data-api.globalforestwatch.org/dataset/birdlife_key_biodiversity_areas/latest/query"
            sql = f"select geom from data where sitrecid in {value_list} order by sitrecid"
        elif aoi_type == "protected_area":
            url = "https://data-api.globalforestwatch.org/dataset/wdpa_protected_areas/latest/query"
            sql = f"select geom from data where wdpaid in {value_list} order by wdpaid"
        elif aoi_type == "indigenous_land":
            url = "https://data-api.globalforestwatch.org/dataset/landmark_ip_lc_and_indicative_poly/latest/query"
            sql = f"select geom from data where landmark_id in {value_list} order by landmark_id"
        else:
            raise ValueError(f"Unable to retrieve AOI type {aoi_type} from Data API.")
        return url, {"sql": sql}

    async def _send_request(self, url, params):
        params["x-api-key"] = self._get_api_key()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(url, params=params)
        return response.json()

    @staticmethod
    def _get_api_key():
        return os.environ["API_KEY"]

    @staticmethod
    def _get_sql_in_list(iter: Iterable) -> str:
        quoted = [f"'{item}'" for item in iter]
        joined = f"({', '.join(quoted)})"
        return joined
