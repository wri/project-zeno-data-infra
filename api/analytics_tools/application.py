import asyncio
from contextlib import asynccontextmanager

import httpx
from mcp.server.transport_security import TransportSecuritySettings

from analytics_tools.analysis_runner import AnalysisRunner
from analytics_tools.analytics_client import AnalyticsClient
from analytics_tools.bearer_token import RequireBearerToken
from analytics_tools.http_analytics_client import HttpAnalyticsClient
from analytics_tools.server import AnalyticsTools, build_server


class AnalyticsToolsApplication:
    """Serves the analytics tools over the streamable HTTP transport, calling
    the analytics API at the given base URL with each caller's bearer token.

    Must be run inside the host application's lifespan, since mounted
    applications don't run their own."""

    def __init__(self, base_url: str, max_wait_seconds: float):
        self.http_client = httpx.AsyncClient(base_url=base_url, timeout=30)

        def client_for(authorization: str | None) -> AnalyticsClient:
            return HttpAnalyticsClient(self.http_client, authorization)

        def runner_for(client: AnalyticsClient) -> AnalysisRunner:
            return AnalysisRunner(client, asyncio.sleep, max_wait_seconds)

        self.server = build_server(AnalyticsTools(client_for, runner_for))
        # Stateless so any instance behind the load balancer can serve a call.
        # Host checks guard local servers against DNS rebinding; this one is
        # public like the rest of the API.
        self.asgi_app = RequireBearerToken(
            self.server.streamable_http_app(
                streamable_http_path="/",
                stateless_http=True,
                json_response=True,
                transport_security=TransportSecuritySettings(
                    enable_dns_rebinding_protection=False
                ),
            )
        )

    @asynccontextmanager
    async def running(self):
        async with self.http_client, self.server.session_manager.run():
            yield
