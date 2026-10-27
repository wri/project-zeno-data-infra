from contextlib import contextmanager
from dataclasses import asdict
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from analytics_tools.analysis_runner import AnalysisRunner
from analytics_tools.analytics_client import AnalyticsClient, AnalyticsRequestError
from analytics_tools.catalog import ApiCatalog, UnknownDatasetError

INSTRUCTIONS = """Land change analytics for areas of interest.
Call list_datasets, then describe_dataset for the request schema of the chosen
dataset, then run_analysis with a payload matching that schema. If the analysis
is still pending when run_analysis returns, call get_analysis with its link."""


@contextmanager
def errors_visible_to_model():
    try:
        yield
    except (AnalyticsRequestError, UnknownDatasetError) as error:
        raise ToolError(str(error)) from error


class AnalyticsTools:
    def __init__(self, client: AnalyticsClient, runner: AnalysisRunner):
        self.client = client
        self.runner = runner

    async def list_datasets(self) -> list[dict[str, Any]]:
        """List the datasets that can be analyzed."""
        catalog = await self.catalog()
        return [asdict(dataset) for dataset in catalog.datasets()]

    async def describe_dataset(self, dataset: str) -> dict[str, Any]:
        """Describe a dataset, its cautions and the payload schema it accepts."""
        with errors_visible_to_model():
            catalog = await self.catalog()
            return asdict(catalog.describe(dataset))

    async def run_analysis(
        self, dataset: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Run an analysis of a dataset with a payload matching its schema."""
        with errors_visible_to_model():
            catalog = await self.catalog()
            path = catalog.describe(dataset).path
            return asdict(await self.runner.run(path, payload))

    async def get_analysis(self, link: str) -> dict[str, Any]:
        """Check on an analysis that was still pending, using its link."""
        with errors_visible_to_model():
            return asdict(await self.runner.wait(link))

    async def catalog(self) -> ApiCatalog:
        return ApiCatalog(await self.client.openapi_document())


def build_server(tools: AnalyticsTools) -> MCPServer:
    server = MCPServer(name="gnw-analytics", instructions=INSTRUCTIONS)
    server.add_tool(tools.list_datasets)
    server.add_tool(tools.describe_dataset)
    server.add_tool(tools.run_analysis)
    server.add_tool(tools.get_analysis)
    return server
