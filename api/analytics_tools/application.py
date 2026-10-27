import asyncio
from contextlib import asynccontextmanager

import httpx
from mcp.server.transport_security import TransportSecuritySettings

from analytics_tools.analysis_runner import AnalysisRunner
from analytics_tools.http_analytics_client import HttpAnalyticsClient
from analytics_tools.server import AnalyticsTools, build_server


class AnalyticsToolsApplication:
    """Serves the analytics tools over the streamable HTTP transport, calling
    the analytics API at the given base URL.

    Must be run inside the host application's lifespan, since mounted
    applications don't run their own."""

    def __init__(self, base_url: str, max_wait_seconds: float):
        self.http_client = httpx.AsyncClient(base_url=base_url, timeout=30)
        client = HttpAnalyticsClient(self.http_client)
        runner = AnalysisRunner(client, asyncio.sleep, max_wait_seconds)
        self.server = build_server(AnalyticsTools(client, runner))
        # Stateless so any instance behind the load balancer can serve a call.
        # Host checks guard local servers against DNS rebinding; this one is
        # public like the rest of the API.
        self.asgi_app = self.server.streamable_http_app(
            streamable_http_path="/",
            stateless_http=True,
            json_response=True,
            transport_security=TransportSecuritySettings(
                enable_dns_rebinding_protection=False
            ),
        )

    @asynccontextmanager
    async def running(self):
        async with self.http_client, self.server.session_manager.run():
            yield
