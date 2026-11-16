import asyncio
from contextlib import asynccontextmanager

import httpx
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from mcp_server.analysis_runner import AnalysisRunner
from mcp_server.http_analytics_client import HttpAnalyticsClients
from mcp_server.server import McpTools, build_server


class McpApplication:
    """Serves the analytics tools over the streamable HTTP transport, calling
    the analytics API at the given base URL with each caller's bearer token.

    Mounted applications don't run their own lifespan, so the host application
    runs this one in its lifespan. Each run starts a fresh server, since its
    connection pool and session manager can only be started once."""

    def __init__(self, base_url: str, max_wait_seconds: float):
        self.base_url = base_url
        self.max_wait_seconds = max_wait_seconds
        self.running_app: ASGIApp | None = None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if self.running_app is None:
            response = PlainTextResponse("MCP server is not running", 503)
            await response(scope, receive, send)
            return
        await self.running_app(scope, receive, send)

    @asynccontextmanager
    async def running(self):
        async with httpx.AsyncClient(base_url=self.base_url, timeout=30) as http:
            tools = McpTools(
                HttpAnalyticsClients(http),
                AnalysisRunner(asyncio.sleep, self.max_wait_seconds),
            )
            server = build_server(tools)
            # Stateless so any instance behind the load balancer can serve a
            # call. Host checks guard local servers against DNS rebinding; this
            # one is public like the rest of the API.
            app = server.streamable_http_app(
                streamable_http_path="/",
                stateless_http=True,
                json_response=True,
                transport_security=TransportSecuritySettings(
                    enable_dns_rebinding_protection=False
                ),
            )
            async with server.session_manager.run():
                self.running_app = app
                try:
                    yield
                finally:
                    self.running_app = None
