import requests
import os

from app.infrastructure.external_services.compute_service import ComputeService


class DataApiComputeService(ComputeService):
    def __init__(self, api_key):
        self.headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key,
        }

    async def compute(self, payload: dict):
        url = f"https://data-api.globalforestwatch.org/dataset/{payload["dataset"]}/{payload["version"]}/query/json"
        params = {
            'sql': payload["query"],
        }

        try:
            response = requests.get(url, headers=self.headers, params=params)

            if response.status_code == 200:
                data = response.json()  # Parse JSON response
                return data["data"]

            raise Exception(f"Error: HTTP {response.status_code}\nResponse: {response.text}")
        except Exception as e:
            raise Exception
