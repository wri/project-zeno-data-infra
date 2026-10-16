import logging
import os
from typing import Iterable

import httpx
from shapely import wkb

from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository


class DataApiAoiGeometryRepository(AoiGeometryRepository):
    """Reads AOI geometries from the latest version of each dataset in the GFW
    Data API."""

    def __init__(self, send_request=None, timeout_seconds: float = 60):
        self.send_request = send_request or self._send_request
        self.timeout_seconds = timeout_seconds

    async def load(self, aoi):
        dataset, id_column = self._dataset_for(aoi.type)
        url = f"https://data-api.globalforestwatch.org/dataset/{dataset}/latest/query"
        sql = (
            f"select {id_column}, geom, gfw_area__ha from data"
            f" where {id_column} in {self._sql_in_list(aoi.ids)}"
        )
        response = await self.send_request(url, {"sql": sql})

        if "data" not in response:
            logging.error(
                {
                    "event": "data_api_geometry_request_failed",
                    "aoi_type": aoi.type,
                    "response": response,
                }
            )
            raise ValueError("Unable to get geometries from Data API.")

        rows = {str(row[id_column]): row for row in response["data"]}
        missing = [aoi_id for aoi_id in aoi.ids if aoi_id not in rows]
        if missing:
            raise ValueError(f"AOI ids not found: {missing}")

        geometries = [wkb.loads(bytes.fromhex(rows[i]["geom"])) for i in aoi.ids]
        areas_ha = [rows[aoi_id].get("gfw_area__ha", 0) for aoi_id in aoi.ids]
        return geometries, areas_ha

    @staticmethod
    def _dataset_for(aoi_type: str):
        datasets = {
            "key_biodiversity_area": ("birdlife_key_biodiversity_areas", "sitrecid"),
            "protected_area": ("wdpa_protected_areas", "site_pid"),
            "indigenous_land": ("landmark_ip_lc_and_indicative_poly", "landmark_id"),
        }
        if aoi_type not in datasets:
            raise ValueError(f"Unable to retrieve AOI type {aoi_type} from Data API.")
        return datasets[aoi_type]

    async def _send_request(self, url, params):
        params["x-api-key"] = os.environ["API_KEY"]
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=self.timeout_seconds
        ) as client:
            response = await client.get(url, params=params)
        return response.json()

    @staticmethod
    def _sql_in_list(values: Iterable[str]) -> str:
        quoted = [f"""'{str(value).replace("'", "''")}'""" for value in values]
        return f"({', '.join(quoted)})"
