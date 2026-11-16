import httpx
import pytest

from mcp_server.application import McpApplication

LIST_TOOLS = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}


async def list_tools(application):
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.post(
            "/",
            json=LIST_TOOLS,
            headers={"Accept": "application/json, text/event-stream"},
        )


@pytest.mark.asyncio
async def test_serves_requests_each_time_the_host_application_starts():
    application = McpApplication(base_url="http://api", max_wait_seconds=1)

    for _ in range(2):
        async with application.running():
            response = await list_tools(application)

        assert response.status_code == 200
        assert "run_analysis" in response.text
