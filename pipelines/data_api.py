from typing import Optional

import httpx

DATA_API_URL = "https://data-api.globalforestwatch.org"


class DataApiClient:
    """Minimal GFW Data API client for reading, creating and updating datasets.

    Requests are authenticated with the given bearer token, which is only needed
    for creating and updating (reads like get_latest_version are public). Any
    non-2xx response raises httpx.HTTPStatusError (except a missing version, see
    get_version). Responses are unwrapped to their "data" field.
    """

    def __init__(
        self,
        token: Optional[str] = None,
        base_url: str = DATA_API_URL,
        http_client: Optional[httpx.Client] = None,
    ):
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.http_client = http_client or httpx.Client(timeout=60)

    def get_latest_version(self, dataset: str) -> str:
        """Return the name of the dataset's latest version.

        Raises ValueError if the dataset has no latest version set.
        """
        response = self._request("GET", f"{self.base_url}/dataset/{dataset}/latest")
        response.raise_for_status()
        body = response.json()
        if body["status"] != "success":
            raise ValueError(f"No latest version set for dataset {dataset}")
        return body["data"]["version"]

    def get_version(self, dataset: str, version: str) -> Optional[dict]:
        """Return the version's metadata, or None if the version doesn't exist."""
        response = self._request("GET", self._version_url(dataset, version))
        if response.status_code == 404:
            return None
        return self._data(response)

    def create_version(self, dataset: str, version: str, body: dict) -> dict:
        return self._data(
            self._request("PUT", self._version_url(dataset, version), json=body)
        )

    def update_version(self, dataset: str, version: str, body: dict) -> dict:
        return self._data(
            self._request("PATCH", self._version_url(dataset, version), json=body)
        )

    def get_assets(self, dataset: str, version: str) -> list[dict]:
        return self._data(
            self._request("GET", f"{self._version_url(dataset, version)}/assets")
        )

    def create_asset(self, dataset: str, version: str, body: dict) -> dict:
        return self._data(
            self._request(
                "POST", f"{self._version_url(dataset, version)}/assets", json=body
            )
        )

    def _version_url(self, dataset: str, version: str) -> str:
        return f"{self.base_url}/dataset/{dataset}/{version}"

    def _request(self, method: str, url: str, json: Optional[dict] = None):
        return self.http_client.request(
            method, url, json=json, headers=self.headers, follow_redirects=True
        )

    @staticmethod
    def _data(response: httpx.Response):
        response.raise_for_status()
        return response.json()["data"]
