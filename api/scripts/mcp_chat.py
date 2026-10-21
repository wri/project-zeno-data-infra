"""Chat in the terminal with Claude, which can call the analytics MCP tools.

Start the API first, then from the api directory run:
    uv run --env-file ../.env python scripts/mcp_chat.py
"""

import argparse
import asyncio
import json

from anthropic import AsyncAnthropic
from anthropic.lib.tools.mcp import async_mcp_tool
from mcp.client.client import Client

SYSTEM_PROMPT = """You help people analyze land change, such as tree cover loss,
disturbance alerts and land cover, for areas of interest. Use the analytics
tools to get real numbers rather than answering from memory, and mention the
dataset's cautions when they affect how a result should be read."""


def show(message) -> None:
    for block in message.content:
        if block.type == "text":
            print(f"\nclaude> {block.text}")
        elif block.type == "tool_use":
            print(f"\n  [calling {block.name} {json.dumps(block.input)[:200]}]")
    if message.stop_reason == "refusal":
        print("\n  [the model declined to answer this request]")


async def read_line(prompt: str) -> str | None:
    try:
        return await asyncio.to_thread(input, prompt)
    except EOFError:
        return None


async def chat(server_url: str, model: str) -> None:
    anthropic_client = AsyncAnthropic()
    async with Client(server_url, read_timeout_seconds=180) as mcp_client:
        listed = await mcp_client.list_tools()
        tools = [async_mcp_tool(tool, mcp_client.session) for tool in listed.tools]
        print(f"Connected to {server_url} with tools: {[t.name for t in listed.tools]}")
        print("Type a question, or press Ctrl-D to quit.")

        # The runner keeps its own copy of the conversation, so the full
        # history is mirrored here to carry it across turns.
        messages: list = []
        while (user_input := await read_line("\nyou> ")) is not None:
            if not user_input.strip():
                continue
            messages.append({"role": "user", "content": user_input})
            runner = anthropic_client.beta.messages.tool_runner(
                model=model,
                max_tokens=16000,
                thinking={"type": "adaptive"},
                system=SYSTEM_PROMPT,
                tools=tools,
                messages=messages,
                # Retries on another model if a safety classifier refuses.
                betas=["server-side-fallback-2026-07-01"],
                fallbacks="default",
            )
            async for message in runner:
                show(message)
                messages.append({"role": "assistant", "content": message.content})
                tool_response = await runner.generate_tool_call_response()
                if tool_response is not None:
                    messages.append(tool_response)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-url", default="http://localhost:8000/mcp/")
    parser.add_argument("--model", default="claude-opus-5-5")
    arguments = parser.parse_args()
    asyncio.run(chat(arguments.server_url, arguments.model))


if __name__ == "__main__":
    main()
