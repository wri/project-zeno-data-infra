import pytest
from mcp.server.mcpserver.exceptions import ToolError

from analytics_tools.analysis_runner import AnalysisRunner
from analytics_tools.analytics_client import AnalysisState
from analytics_tools.server import AnalyticsTools, build_server

LINK = "http://api/v0/land_change/tree_cover_loss/analytics/abc"
OPENAPI_DOCUMENT = {
    "paths": {
        "/v0/land_change/tree_cover_loss/analytics": {
            "post": {
                "summary": "Tree Cover Loss",
                "requestBody": {
                    "content": {"application/json": {"schema": {"type": "object"}}}
                },
            }
        }
    }
}


class FakeClient:
    def __init__(self):
        self.created = []

    async def openapi_document(self):
        return OPENAPI_DOCUMENT

    async def create_analysis(self, path, payload):
        self.created.append((path, payload))
        return LINK

    async def get_analysis(self, link):
        return AnalysisState(
            link=link,
            status="saved",
            message="Analysis completed successfully.",
            retry_after_seconds=None,
            result={"area_ha": [1.5]},
            metadata=None,
        )


async def no_sleep(seconds):
    pass


def server_with(client):
    runner = AnalysisRunner(client, no_sleep, max_wait_seconds=10)
    return build_server(AnalyticsTools(client, runner))


@pytest.mark.asyncio
async def test_run_analysis_posts_to_the_dataset_endpoint_and_returns_result():
    client = FakeClient()
    server = server_with(client)

    result = await server.call_tool(
        "run_analysis", {"dataset": "tree_cover_loss", "payload": {"aoi": {}}}
    )

    assert client.created == [
        ("/v0/land_change/tree_cover_loss/analytics", {"aoi": {}})
    ]
    assert result.structured_content["result"] == {"area_ha": [1.5]}


@pytest.mark.asyncio
async def test_expected_failures_reach_the_model_with_their_message():
    server = server_with(FakeClient())

    with pytest.raises(ToolError, match="Available datasets: tree_cover_loss"):
        await server.call_tool("describe_dataset", {"dataset": "tree_cover"})
