from typing import Any, Callable, Dict


class PrecomputedAdminSource:
    """Serves zonal statistics for admin AOIs from a precomputed table.

    `build_query` is a dataset-specific function turning an analytics request
    into the SQL run against the injected query service.
    """

    def __init__(self, query_service, build_query: Callable[[Any], str]):
        self.query_service = query_service
        self.build_query = build_query

    async def get(self, analytics_in) -> Dict[str, Any]:
        data: Dict = await self.query_service.execute(self.build_query(analytics_in))
        data["aoi_type"] = ["admin"] * len(data["aoi_id"])
        return data
