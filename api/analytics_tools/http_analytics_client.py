from typing import Any

import httpx

from analytics_tools.analytics_client import (
    AnalysisState,
    AnalyticsRequestError,
    AreaMatch,
)


class HttpAnalyticsClient:
    """Talks to the analytics API over HTTP on behalf of one caller, whose
    authorization is sent with every request. Only links on the API's own base
    URL are followed, since links can come from untrusted tool input."""

    def __init__(self, http_client: httpx.AsyncClient, authorization: str | None):
        self.http_client = http_client
        self.headers = {"Authorization": authorization} if authorization else {}

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
        if not link.startswith(str(self.http_client.base_url)):
            raise AnalyticsRequestError(f"Link is not an analytics API link: {link}")
        response = await self.http_client.get(link, headers=self.headers)
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
