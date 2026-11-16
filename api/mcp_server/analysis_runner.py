from typing import Any, Awaitable, Callable

from mcp_server.analytics_client import AnalysisState, AnalyticsClient


class AnalysisRunner:
    """Starts an analysis and polls it until it finishes or the wait limit is
    reached, in which case the still-pending state is returned so the caller
    can check back later using its link."""

    def __init__(
        self,
        sleep: Callable[[float], Awaitable[None]],
        max_wait_seconds: float,
    ):
        self.sleep = sleep
        self.max_wait_seconds = max_wait_seconds

    async def run(
        self, client: AnalyticsClient, path: str, payload: dict[str, Any]
    ) -> AnalysisState:
        link = await client.create_analysis(path, payload)
        # Give the analysis a moment to start, since it isn't reported as
        # pending until then.
        await self.sleep(1)
        return await self.wait(client, link)

    async def wait(self, client: AnalyticsClient, link: str) -> AnalysisState:
        remaining_seconds = self.max_wait_seconds
        state = await client.get_analysis(link)
        while not state.is_finished:
            delay = state.retry_after_seconds or 1
            if delay > remaining_seconds:
                break
            await self.sleep(delay)
            remaining_seconds -= delay
            state = await client.get_analysis(link)
        return state
