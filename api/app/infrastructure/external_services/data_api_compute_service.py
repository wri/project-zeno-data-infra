import logging
import os
import requests

from app.infrastructure.external_services.compute_service import ComputeService


class DataApiComputeService(ComputeService):
    def __init__(self, api_key):
        self.headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key,
        }

    async def compute(self, payload: dict):
        logging.info(
            {
                "event": "data_api_compute_request",
                "payload": payload,
            }
        )

        url = f"https://data-api.globalforestwatch.org/dataset/{payload["dataset"]}/{payload["version"]}/query/json"
        params = { "sql": payload["query"] }

        try:
            response = requests.get(url, headers=self.headers, params=params)

            if response.status_code == 200:
                return response.json()["data"]

            raise Exception(f"Error: HTTP {response.status_code}\nResponse: {response.text}")
        except Exception as e:
            logging.error(
                {
                    "event": "data_api_compute_service_failure",
                    "severity": "high",
                    "metadata": payload,
                    "error_type": e.__class__.__name__,
                    "error_details": str(e),
                    "stack_trace": traceback.format_exc(),
                }
            )
            raise Exception("Data API Compute Service Failure")
