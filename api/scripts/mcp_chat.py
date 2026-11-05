"""Chat in the terminal with Claude, which can call the analytics MCP tools.

Start the API first, then from the api directory run:
    uv run --env-file ../.env python scripts/mcp_chat.py

Requires ANTHROPIC_API_KEY and RW_AUTH_TOKEN, which is forwarded to the API.
"""

import argparse
import asyncio
import json
import os

from analytics_agent import connect_agent


def show(message) -> None:
    for block in message.content:
        if block.type == "thinking" and block.thinking:
            print(f"\n  [thinking]\n{block.thinking}")
        elif block.type == "text":
            print(f"\nclaude> {block.text}")
        elif block.type == "tool_use":
            print(f"\n  [calling {block.name} {json.dumps(block.input)[:200]}]")
        elif block.type == "server_tool_use":
            print(f"\n  [running script]\n{block.input.get('code', '')}")
        elif block.type == "code_execution_tool_result":
            print(f"\n  [script output]\n{getattr(block.content, 'stdout', '')}")
    if message.stop_reason == "refusal":
        print("\n  [the model declined to answer this request]")


async def read_line(prompt: str) -> str | None:
    try:
        return await asyncio.to_thread(input, prompt)
    except EOFError:
        return None


async def chat(
    server_url: str, model: str, bearer_token: str, programmatic_tools: bool
) -> None:
    async with connect_agent(
        server_url, model, bearer_token, programmatic_tools
    ) as agent:
        print(f"Connected to {server_url}")
        print("Type a question, or press Ctrl-D to quit.")
        messages: list = []
        while (user_input := await read_line("\nyou> ")) is not None:
            if not user_input.strip():
                continue
            messages.append({"role": "user", "content": user_input})
            await agent.reply(messages, on_message=show)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-url", default="http://localhost:8000/mcp/")
    parser.add_argument("--model", default="claude-opus-5-5")
    parser.add_argument(
        "--programmatic-tools",
        action="store_true",
        help="Let Claude call the tools from scripts it runs in a sandbox",
    )
    arguments = parser.parse_args()
    asyncio.run(
        chat(
            arguments.server_url,
            arguments.model,
            os.environ["RW_AUTH_TOKEN"],
            arguments.programmatic_tools,
        )
    )


if __name__ == "__main__":
    main()
