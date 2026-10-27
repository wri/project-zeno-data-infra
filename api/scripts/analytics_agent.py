"""A Claude agent that answers land change questions using the analytics MCP
tools. Shared by the chat CLI and the evals."""

from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable

import httpx2
from anthropic import AsyncAnthropic
from anthropic.lib.tools.mcp import async_mcp_tool
from mcp.client.client import Client
from mcp.client.streamable_http import streamable_http_client

SYSTEM_PROMPT = """You help people analyze land change, such as tree cover loss,
disturbance alerts and land cover, for areas of interest. Use the analytics
tools to get real numbers rather than answering from memory, and mention the
dataset's cautions when they affect how a result should be read."""


class AnalyticsAgent:
    def __init__(self, anthropic_client: AsyncAnthropic, tools: list, model: str):
        self.anthropic_client = anthropic_client
        self.tools = tools
        self.model = model

    async def reply(self, messages: list, on_message: Callable | None = None) -> None:
        """Answers the conversation's last user message, appending each
        assistant turn and tool result to the messages."""
        runner = self.anthropic_client.beta.messages.tool_runner(
            model=self.model,
            max_tokens=16000,
            thinking={"type": "adaptive"},
            system=SYSTEM_PROMPT,
            tools=self.tools,
            messages=messages,
            # Retries on another model if a safety classifier refuses.
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
        )
        # The runner keeps its own copy of the conversation, so turns are
        # mirrored into the caller's messages to carry them forward.
        async for message in runner:
            if on_message is not None:
                on_message(message)
            messages.append({"role": "assistant", "content": message.content})
            tool_response = await runner.generate_tool_call_response()
            if tool_response is not None:
                messages.append(tool_response)


@asynccontextmanager
async def connect_agent(
    server_url: str, model: str, bearer_token: str
) -> AsyncIterator[AnalyticsAgent]:
    """Connects to the MCP server, sending the bearer token it forwards to the
    analytics API, and yields an agent that uses its tools."""
    http_client = httpx2.AsyncClient(
        headers={"Authorization": f"Bearer {bearer_token}"},
        timeout=httpx2.Timeout(30, read=300),
    )
    transport = streamable_http_client(server_url, http_client=http_client)
    async with http_client, Client(transport, read_timeout_seconds=180) as mcp_client:
        listed = await mcp_client.list_tools()
        tools = [async_mcp_tool(tool, mcp_client.session) for tool in listed.tools]
        yield AnalyticsAgent(AsyncAnthropic(), tools, model)
