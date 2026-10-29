"""Run the gold eval cases against the analytics MCP agent.

For each case, checks from the conversation whether the agent analyzed an
expected dataset and the expected areas, and asks a judge model whether its
final answer matches the expected answer. Start the API first, then from the
api directory run:
    uv run --env-file ../.env python scripts/run_evals.py

Requires ANTHROPIC_API_KEY and RW_AUTH_TOKEN.
"""

import argparse
import asyncio
import csv
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from analytics_agent import AnalyticsAgent, connect_agent
from anthropic import AsyncAnthropic
from pydantic import BaseModel

JUDGE_PROMPT = """You are grading an answer to a land change data question.

Question: {question}
Expected answer: {expected}
Agent's final answer:
<answer>
{answer}
</answer>

Decide whether the agent's headline answer agrees with the expected answer.
Numbers agree when within 2% of each other after converting units (for example
hectares vs. thousand hectares, or different number formats). Yes/no, true/false,
years and categories agree when they mean the same thing. Extra context in the
agent's answer is fine; a different or missing headline answer is not."""


class Verdict(BaseModel):
    agent_value: str
    correct: bool
    reason: str


@dataclass
class AnalysisCalls:
    """What the agent's successful analyses covered."""

    datasets: set[str] = field(default_factory=set)
    aoi_ids: set[str] = field(default_factory=set)


def successful_analyses(messages: list) -> AnalysisCalls:
    calls = {}
    for message in messages:
        if message["role"] != "assistant":
            continue
        for block in message["content"]:
            if block.type == "tool_use" and block.name == "run_analysis":
                calls[block.id] = block.input

    analyses = AnalysisCalls()
    for message in messages:
        if message["role"] != "user" or isinstance(message["content"], str):
            continue
        for block in message["content"]:
            call = calls.get(block.get("tool_use_id"))
            if call is None or block.get("is_error"):
                continue
            payload = call.get("payload", {})
            dataset = call.get("dataset", "")
            analyses.datasets.add(dataset)
            for intersection in payload.get("intersections") or []:
                analyses.datasets.add(f"{dataset}:{intersection}")
            for aoi_id in (payload.get("aoi") or {}).get("ids", []):
                analyses.aoi_ids.add(aoi_id.upper())
    return analyses


def transcript(messages: list) -> list[dict]:
    """The conversation as plain data: tool calls with their inputs, tool
    results (truncated) and the agent's text."""
    entries = []
    for message in messages:
        if isinstance(message["content"], str):
            entries.append({"role": message["role"], "text": message["content"]})
            continue
        for block in message["content"]:
            if isinstance(block, dict):
                entries.append(
                    {
                        "tool_result": block.get("tool_use_id"),
                        "is_error": block.get("is_error", False),
                        "content": json.dumps(block.get("content"), default=str)[:2000],
                    }
                )
            elif block.type == "tool_use":
                entries.append(
                    {"tool_call": block.id, "name": block.name, "input": block.input}
                )
            elif block.type == "text":
                entries.append({"role": "assistant", "text": block.text})
    return entries


def final_text(messages: list) -> str:
    for message in reversed(messages):
        if message["role"] == "assistant":
            texts = [b.text for b in message["content"] if b.type == "text"]
            if texts:
                return "\n".join(texts)
    return ""


def used_expected_dataset(case: dict, analyses: AnalysisCalls) -> bool:
    return any(d in analyses.datasets for d in case["datasets"].split(";"))


def used_expected_areas(case: dict, analyses: AnalysisCalls) -> bool | None:
    expected = [aoi_id.upper() for aoi_id in case["aoi_ids"].split(";") if aoi_id]
    if not expected:
        return None
    return all(aoi_id in analyses.aoi_ids for aoi_id in expected)


async def judge(client: AsyncAnthropic, model: str, case: dict, answer: str):
    response = await client.messages.parse(
        model=model,
        max_tokens=1024,
        messages=[
            {
                "role": "user",
                "content": JUDGE_PROMPT.format(
                    question=case["question"], expected=case["answer"], answer=answer
                ),
            }
        ],
        output_format=Verdict,
    )
    return response.parsed_output


class ResultWriter:
    """Writes each case's result row and transcript as soon as the case
    finishes, so an interrupted run keeps everything completed so far."""

    def __init__(self, output: Path, fieldnames: list[str]):
        self.output = output
        self.csv_file = output.open("w", newline="")
        self.rows = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        self.rows.writeheader()
        self.transcripts = output.with_suffix(".jsonl").open("w")

    def write(self, result: dict, conversation: dict) -> None:
        self.rows.writerow(result)
        self.csv_file.flush()
        self.transcripts.write(json.dumps(conversation, default=str) + "\n")
        self.transcripts.flush()

    def close(self) -> None:
        self.csv_file.close()
        self.transcripts.close()


RESULT_COLUMNS = [
    "dataset_ok",
    "areas_ok",
    "answer_ok",
    "agent_value",
    "judge_reason",
    "datasets_used",
    "aoi_ids_used",
    "tool_calls",
    "error",
    "final_answer",
]


async def run_case(
    agent: AnalyticsAgent,
    judge_client: AsyncAnthropic,
    judge_model: str,
    case: dict,
    limit: asyncio.Semaphore,
    writer: ResultWriter,
) -> dict:
    async with limit:
        messages: list = [{"role": "user", "content": case["question"]}]
        errors = []
        try:
            await agent.reply(messages)
        except Exception as exception:
            errors.append(f"agent {type(exception).__name__}: {exception}")
        analyses = successful_analyses(messages)
        answer = final_text(messages)
        verdict = None
        try:
            verdict = await judge(judge_client, judge_model, case, answer)
        except Exception as exception:
            errors.append(f"judge {type(exception).__name__}: {exception}")
        outcome = (
            "judge failed" if verdict is None else "ok" if verdict.correct else "wrong"
        )
        print(f"  {case['case_id']}: answer {outcome}", flush=True)
        result = {
            **case,
            "dataset_ok": used_expected_dataset(case, analyses),
            "areas_ok": used_expected_areas(case, analyses),
            "answer_ok": verdict.correct if verdict else None,
            "agent_value": verdict.agent_value if verdict else "",
            "judge_reason": verdict.reason if verdict else "",
            "datasets_used": ";".join(sorted(analyses.datasets)),
            "aoi_ids_used": ";".join(sorted(analyses.aoi_ids)),
            "tool_calls": sum(
                1
                for m in messages
                if m["role"] == "assistant"
                for b in m["content"]
                if b.type == "tool_use"
            ),
            "error": "; ".join(errors),
            "final_answer": answer,
        }
        writer.write(
            result, {"case_id": case["case_id"], "transcript": transcript(messages)}
        )
        return result


def pass_rate(results: list[dict], check: str) -> str:
    scored = [r[check] for r in results if r[check] is not None]
    if not scored:
        return "n/a"
    return f"{sum(scored)}/{len(scored)} ({100 * sum(scored) / len(scored):.0f}%)"


async def run(arguments: argparse.Namespace) -> None:
    with open(arguments.cases) as file:
        cases = list(csv.DictReader(file))
    if arguments.limit:
        cases = cases[: arguments.limit]
    print(f"Running {len(cases)} cases", flush=True)

    results_dir = Path(arguments.cases).parent / "results"
    results_dir.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    writer = ResultWriter(results_dir / f"{stamp}.csv", list(cases[0]) + RESULT_COLUMNS)
    print(f"Writing results to {writer.output} as cases finish", flush=True)

    judge_client = AsyncAnthropic()
    limit = asyncio.Semaphore(arguments.concurrency)
    try:
        async with connect_agent(
            arguments.server_url, arguments.model, os.environ["RW_AUTH_TOKEN"]
        ) as agent:
            results = await asyncio.gather(
                *[
                    run_case(
                        agent, judge_client, arguments.judge_model, case, limit, writer
                    )
                    for case in cases
                ]
            )
    finally:
        writer.close()

    print()
    print(f"Dataset used:  {pass_rate(results, 'dataset_ok')}")
    print(f"Areas used:    {pass_rate(results, 'areas_ok')}")
    print(f"Answer right:  {pass_rate(results, 'answer_ok')}")
    errors = [r for r in results if r["error"]]
    if errors:
        print(f"Errors: {len(errors)}")
        print(json.dumps([[r["case_id"], r["error"]] for r in errors], indent=1))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", default="evals/cases.csv")
    parser.add_argument("--server-url", default="http://localhost:8000/mcp/")
    parser.add_argument("--model", default="claude-opus-5-5")
    parser.add_argument("--judge-model", default="claude-haiku-4-5")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--limit", type=int, help="Run only the first N cases")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
