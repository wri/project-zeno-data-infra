from contextlib import contextmanager
from dataclasses import asdict
from typing import Any, Callable

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from analytics_tools.analysis_runner import AnalysisRunner
from analytics_tools.analytics_client import AnalyticsClient, AnalyticsRequestError
from analytics_tools.catalog import ApiCatalog, UnknownDatasetError

INSTRUCTIONS = """Land change analytics for areas of interest.
Call search_areas to find the area a question is about, list_datasets, then
describe_dataset for the request schema of the chosen dataset, then
run_analysis with a payload matching that schema, using the aoi returned by
search_areas. For questions about the regions or protected areas of a place,
call list_subregions and analyze them together in one payload. If the analysis
is still pending when run_analysis returns, call get_analysis with its link."""


@contextmanager
def errors_visible_to_model():
    try:
        yield
    except (AnalyticsRequestError, UnknownDatasetError) as error:
        raise ToolError(str(error)) from error


class AnalyticsTools:
    """Tools that call the analytics API as the caller, using the
    authorization their MCP request carried."""

    def __init__(
        self,
        client_for: Callable[[str | None], AnalyticsClient],
        runner_for: Callable[[AnalyticsClient], AnalysisRunner],
    ):
        self.client_for = client_for
        self.runner_for = runner_for

    async def search_areas(
        self,
        name: str,
        ctx: Context,
        area_types: list[str] | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Find areas by place name, best match first. Each match includes the
        aoi to use in run_analysis payloads. area_types can narrow the search
        to admin, key_biodiversity_area, protected_area or indigenous_land."""
        with errors_visible_to_model():
            client = self.caller_client(ctx)
            matches = await client.search_areas(name, area_types, limit)
            return [asdict(match) for match in matches]

    async def list_subregions(
        self,
        aoi: dict[str, Any],
        ctx: Context,
        area_type: str = "admin",
        level: int | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        """List the areas within an area, largest first, with how many there
        are in all, as aois to use in run_analysis payloads. Use it for
        questions about the regions, states, counties or protected areas of a
        place. area_type is admin, key_biodiversity_area, protected_area or
        indigenous_land; for admin, level is the admin level to list (default:
        one below the area). The admin aoi global lists countries. When total is
        larger than the areas returned, ask for a higher level or a smaller
        area instead of more results."""
        with errors_visible_to_model():
            client = self.caller_client(ctx)
            listed = await client.subregions(aoi, area_type, level, limit)
            return asdict(listed)

    async def list_datasets(self, ctx: Context) -> list[dict[str, Any]]:
        """List the datasets that can be analyzed."""
        with errors_visible_to_model():
            catalog = await self.catalog(self.caller_client(ctx))
            return [asdict(dataset) for dataset in catalog.datasets()]

    async def describe_dataset(self, dataset: str, ctx: Context) -> dict[str, Any]:
        """Describe a dataset, its cautions and the payload schema it accepts."""
        with errors_visible_to_model():
            catalog = await self.catalog(self.caller_client(ctx))
            return asdict(catalog.describe(dataset))

    async def run_analysis(
        self, dataset: str, payload: dict[str, Any], ctx: Context
    ) -> dict[str, Any]:
        """Run an analysis of a dataset with a payload matching its schema."""
        with errors_visible_to_model():
            client = self.caller_client(ctx)
            path = (await self.catalog(client)).describe(dataset).path
            return asdict(await self.runner_for(client).run(path, payload))

    async def get_analysis(self, link: str, ctx: Context) -> dict[str, Any]:
        """Check on an analysis that was still pending, using its link."""
        with errors_visible_to_model():
            runner = self.runner_for(self.caller_client(ctx))
            return asdict(await runner.wait(link))

    def caller_client(self, ctx: Context) -> AnalyticsClient:
        headers = ctx.headers or {}
        return self.client_for(headers.get("authorization"))

    async def catalog(self, client: AnalyticsClient) -> ApiCatalog:
        return ApiCatalog(await client.openapi_document())


def build_server(tools: AnalyticsTools) -> MCPServer:
    server = MCPServer(name="gnw-analytics", instructions=INSTRUCTIONS)
    server.add_tool(tools.search_areas)
    server.add_tool(tools.list_subregions)
    server.add_tool(tools.list_datasets)
    server.add_tool(tools.describe_dataset)
    server.add_tool(tools.run_analysis)
    server.add_tool(tools.get_analysis)
    return server
