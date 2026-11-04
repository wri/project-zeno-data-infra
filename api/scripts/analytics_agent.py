"""A Claude agent that answers land change questions using the analytics MCP
tools. Shared by the chat CLI and the evals."""

from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable

import httpx2
from anthropic import AsyncAnthropic
from anthropic.lib.tools.mcp import async_mcp_tool
from mcp.client.client import Client
from mcp.client.streamable_http import streamable_http_client

CODE_EXECUTION = "code_execution_20260120"

SYSTEM_PROMPT = """You help people analyze land change, such as tree cover loss,
disturbance alerts and land cover, for areas of interest.

Every number and factual claim about an area must come from a tool result in
this conversation. That includes values, rankings, dates and area identifiers:
find areas with search_areas rather than recalling their IDs. Calculations on
tool results are fine; say how you derived them. If the tools can't provide
something, say so rather than filling the gap from memory. People make
decisions from these answers, and a remembered number can't be traced back to
a dataset.

Mention the dataset's cautions when they affect how a result should be read."""


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
            if message.container is not None:
                # A script paused on a tool call resumes only in its own container.
                container_id = message.container.id
                runner.set_messages_params(
                    lambda params: {**params, "container": container_id}
                )
            tool_response = await runner.generate_tool_call_response()
            if tool_response is not None:
                messages.append(tool_response)


@asynccontextmanager
async def connect_agent(
    server_url: str, model: str, bearer_token: str, programmatic_tools: bool = False
) -> AsyncIterator[AnalyticsAgent]:
    """Connects to the MCP server, sending the bearer token it forwards to the
    analytics API, and yields an agent that uses its tools. With programmatic
    tools, the agent can also call the tools from scripts it runs in a sandbox."""
    http_client = httpx2.AsyncClient(
        headers={"Authorization": f"Bearer {bearer_token}"},
        timeout=httpx2.Timeout(30, read=300),
    )
    transport = streamable_http_client(server_url, http_client=http_client)
    async with http_client, Client(transport, read_timeout_seconds=180) as mcp_client:
        listed = await mcp_client.list_tools()
        allowed_callers = ["direct", CODE_EXECUTION] if programmatic_tools else None
        tools: list = [
            async_mcp_tool(tool, mcp_client.session, allowed_callers=allowed_callers)
            for tool in listed.tools
        ]
        if programmatic_tools:
            tools.append({"type": CODE_EXECUTION, "name": "code_execution"})
        yield AnalyticsAgent(AsyncAnthropic(), tools, model)
