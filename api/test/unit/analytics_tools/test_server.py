import httpx
import pytest

from analytics_tools.analysis_runner import AnalysisRunner
from analytics_tools.analytics_client import AnalysisState, AreaMatch
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
        self.searches = []

    async def search_areas(self, name, area_types, limit):
        self.searches.append((name, area_types, limit))
        return [
            AreaMatch(
                name="Acre",
                display_name="Acre, Brazil",
                aoi={"type": "admin", "ids": ["BRA.1"]},
            )
        ]

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


def server_with(client, authorizations=None):
    def client_for(authorization):
        if authorizations is not None:
            authorizations.append(authorization)
        return client

    def runner_for(analytics_client):
        return AnalysisRunner(analytics_client, no_sleep, max_wait_seconds=10)

    return build_server(AnalyticsTools(client_for, runner_for))


async def call_tool(server, name, arguments):
    """Calls a tool over HTTP, as MCP clients do, returning the tool result."""
    app = server.streamable_http_app(
        streamable_http_path="/", stateless_http=True, json_response=True
    )
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }
    async with server.session_manager.run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost:8000"
        ) as http_client:
            response = await http_client.post(
                "/",
                json=request,
                headers={
                    "Authorization": "Bearer caller-token",
                    "Accept": "application/json, text/event-stream",
                },
            )
    return response.json()["result"]


@pytest.mark.asyncio
async def test_run_analysis_posts_to_the_dataset_endpoint_and_returns_result():
    client = FakeClient()
    server = server_with(client)

    result = await call_tool(
        server, "run_analysis", {"dataset": "tree_cover_loss", "payload": {"aoi": {}}}
    )

    assert client.created == [
        ("/v0/land_change/tree_cover_loss/analytics", {"aoi": {}})
    ]
    assert result["structuredContent"]["result"] == {"area_ha": [1.5]}


@pytest.mark.asyncio
async def test_expected_failures_reach_the_model_with_their_message():
    server = server_with(FakeClient())

    result = await call_tool(server, "describe_dataset", {"dataset": "tree_cover"})

    assert result["isError"]
    assert "Available datasets: tree_cover_loss" in result["content"][0]["text"]


@pytest.mark.asyncio
async def test_search_areas_returns_payloads_ready_for_analysis():
    client = FakeClient()
    server = server_with(client)

    result = await call_tool(
        server, "search_areas", {"name": "Acre", "area_types": ["admin"]}
    )

    assert client.searches == [("Acre", ["admin"], 10)]
    assert result["structuredContent"]["result"] == [
        {
            "name": "Acre",
            "display_name": "Acre, Brazil",
            "aoi": {"type": "admin", "ids": ["BRA.1"]},
        }
    ]


@pytest.mark.asyncio
async def test_api_calls_use_the_callers_authorization():
    authorizations: list = []
    server = server_with(FakeClient(), authorizations)

    await call_tool(server, "list_datasets", {})

    assert authorizations == ["Bearer caller-token"]
