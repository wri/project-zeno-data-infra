import pytest

from analytics_tools.analysis_runner import AnalysisRunner
from analytics_tools.analytics_client import AnalysisState

LINK = "http://api/v0/land_change/tree_cover_loss/analytics/abc"


def state(status, retry_after_seconds=None):
    return AnalysisState(
        link=LINK,
        status=status,
        message="",
        retry_after_seconds=retry_after_seconds,
        result={"value": [1]} if status == "saved" else None,
        metadata=None,
    )


class FakeClient:
    def __init__(self, states):
        self.states = list(states)
        self.created_payloads = []

    async def openapi_document(self):
        return {}

    async def create_analysis(self, path, payload):
        self.created_payloads.append(payload)
        return LINK

    async def get_analysis(self, link):
        return self.states.pop(0)


class RecordingSleep:
    def __init__(self):
        self.calls = []

    async def __call__(self, seconds):
        self.calls.append(seconds)


@pytest.mark.asyncio
async def test_polls_following_retry_after_until_saved():
    client = FakeClient([state("pending", 2), state("pending", 3), state("saved")])
    sleep = RecordingSleep()
    runner = AnalysisRunner(client, sleep, max_wait_seconds=30)

    final_state = await runner.run("/path", {"aoi": {}})

    assert final_state.status == "saved"
    assert final_state.result == {"value": [1]}
    assert sleep.calls == [2, 3]


@pytest.mark.asyncio
async def test_returns_pending_link_instead_of_waiting_past_the_limit():
    client = FakeClient([state("pending", 5), state("pending", 5), state("saved")])
    sleep = RecordingSleep()
    runner = AnalysisRunner(client, sleep, max_wait_seconds=8)

    final_state = await runner.run("/path", {"aoi": {}})

    assert final_state.status == "pending"
    assert final_state.link == LINK
    assert sleep.calls == [5]
