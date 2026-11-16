from typing import Any
from urllib.parse import urlsplit

import httpx

from mcp_server.analytics_client import (
    AnalysisState,
    AnalyticsRequestError,
    AreaMatch,
)


class HttpAnalyticsClient:
    """Talks to the analytics API over HTTP on behalf of one caller, sending
    the caller's authorization and host, so the API builds links the same way
    it does for direct callers. Links are followed by their path only, and only
    when they are on the caller's host, since they come from tool input."""

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        authorization: str | None,
        host: str | None,
    ):
        self.http_client = http_client
        self.host = host or http_client.base_url.netloc.decode()
        self.headers = {"Host": self.host}
        if authorization:
            self.headers["Authorization"] = authorization

    async def openapi_document(self) -> dict[str, Any]:
        response = await self.http_client.get("/openapi.json", headers=self.headers)
        return self.json_or_raise(response)

    async def search_areas(
        self, name: str, area_types: list[str] | None, limit: int
    ) -> list[AreaMatch]:
        params: dict[str, Any] = {"q": name, "limit": limit}
        if area_types:
            params["type"] = area_types
        response = await self.http_client.get(
            "/v0/aois/search", params=params, headers=self.headers
        )
        matches = []
        for match in self.json_or_raise(response)["data"]:
            matches.append(
                AreaMatch(
                    name=match["name"],
                    display_name=match["display_name"],
                    aoi={"type": match["aoi_type"], "ids": [match["aoi_id"]]},
                )
            )
        return matches

    async def create_analysis(self, path: str, payload: dict[str, Any]) -> str:
        response = await self.http_client.post(path, json=payload, headers=self.headers)
        return self.json_or_raise(response)["data"]["link"]

    async def get_analysis(self, link: str) -> AnalysisState:
        parts = urlsplit(link)
        if parts.netloc != self.host:
            raise AnalyticsRequestError(f"Link is not an analytics API link: {link}")
        response = await self.http_client.get(parts.path, headers=self.headers)
        data = self.json_or_raise(response)["data"]
        retry_after = response.headers.get("Retry-After")
        return AnalysisState(
            link=link,
            status=data.get("status"),
            message=data.get("message", ""),
            retry_after_seconds=float(retry_after) if retry_after else None,
            result=data.get("result"),
            metadata=data.get("metadata"),
        )

    def json_or_raise(self, response: httpx.Response) -> Any:
        if response.is_error:
            raise AnalyticsRequestError(
                f"Analytics API returned {response.status_code}: {response.text}"
            )
        return response.json()


class HttpAnalyticsClients:
    """Clients for each caller, sharing one connection pool to the API."""

    def __init__(self, http_client: httpx.AsyncClient):
        self.http_client = http_client

    def for_caller(
        self, authorization: str | None, host: str | None
    ) -> HttpAnalyticsClient:
        return HttpAnalyticsClient(self.http_client, authorization, host)
