import pytest

from pipelines.wur_alert_drivers.gee_export import (
    ExportError,
    file_name_prefix,
    latest_asset_path,
    latest_end,
    start_exports_if_needed,
    task_description,
    wait_for_exports,
)

VERSION = "v20261101"
CONTENT_DATE = "2026-09-30"


class FakeGeeClient:
    """In-memory GEE and GCS. list_tasks returns tasks; each get_task call for a
    name returns the next entry of states[name] (the last one repeats). Deletes
    and started exports are recorded."""

    def __init__(self, tasks=None, states=None):
        self.tasks = tasks or []
        self.states = states or {}
        self.deleted = []
        self.started = []

    def list_tasks(self):
        return self.tasks

    def get_task(self, name):
        states = self.states[name]
        state = states.pop(0) if len(states) > 1 else states[0]
        error = "boom" if state == "FAILED" else None
        return {"name": name, "description": name, "state": state, "error": error}

    def delete_tifs(self, prefix):
        self.deleted.append(prefix)
        return 2

    def start_export(self, region, content_date, description, prefix):
        self.started.append((region, content_date, description, prefix))
        return f"op-{region}"


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def _task(region, state, name=None, test=False):
    return {
        "name": name or f"old-{region}",
        "description": task_description(region, VERSION, test),
        "state": state,
        "error": None,
    }


def test_latest_end_is_day_after_content_date():
    assert latest_end("2026-09-30") == "20261001"
    assert latest_end("2026-12-31") == "20270101"


def test_latest_asset_path():
    assert latest_asset_path("sa", CONTENT_DATE) == (
        "projects/wurnrt-drivers/assets/operational_postprocessed_sa_collection/"
        "driverclassification_postprocessed_sa_mosaic_20260101_20261001"
    )


def test_file_name_prefix():
    assert file_name_prefix("sea") == "wur_alert_drivers/wur_alert_drivers_encoded_sea"
    assert (
        file_name_prefix("sea", test=True)
        == "wur_alert_drivers/test/wur_alert_drivers_encoded_sea"
    )


def test_starts_all_regions_when_no_tasks():
    client = FakeGeeClient()
    names = start_exports_if_needed(client, VERSION, CONTENT_DATE)
    assert names == ["op-afr", "op-sa", "op-sea"]
    assert client.deleted == [file_name_prefix(r) for r in ("afr", "sa", "sea")]
    assert client.started[1] == (
        "sa",
        CONTENT_DATE,
        "wur_alert_drivers_encoded_sa_v20261101",
        "wur_alert_drivers/wur_alert_drivers_encoded_sa",
    )


def test_resumes_from_existing_tasks():
    client = FakeGeeClient(
        tasks=[
            _task("afr", "SUCCEEDED"),
            _task("sa", "RUNNING", name="running-sa"),
            _task("sea", "FAILED"),
            # Other versions and test exports don't count.
            {**_task("sea", "SUCCEEDED"), "description": "other"},
            _task("sea", "SUCCEEDED", test=True),
        ]
    )
    names = start_exports_if_needed(client, VERSION, CONTENT_DATE)
    assert names == ["running-sa", "op-sea"]
    assert client.deleted == [file_name_prefix("sea")]
    assert [s[0] for s in client.started] == ["sea"]


def test_succeeded_retry_wins_over_earlier_failure():
    client = FakeGeeClient(
        tasks=[_task("afr", "FAILED"), _task("afr", "SUCCEEDED")]
        + [_task(r, "SUCCEEDED") for r in ("sa", "sea")]
    )
    assert start_exports_if_needed(client, VERSION, CONTENT_DATE) == []
    assert client.deleted == [] and client.started == []


def test_test_exports_go_to_test_folder():
    client = FakeGeeClient()
    start_exports_if_needed(client, VERSION, CONTENT_DATE, test=True)
    assert client.deleted[0] == "wur_alert_drivers/test/wur_alert_drivers_encoded_afr"
    assert client.started[0][2] == "wur_alert_drivers_encoded_afr_v20261101_test"


def _wait(client, names, timeout_seconds=3600):
    clock = FakeClock()
    wait_for_exports(
        client, names, timeout_seconds, poll_seconds=300, sleep=clock.sleep, clock=clock
    )
    return clock


def test_wait_until_all_succeed():
    client = FakeGeeClient(
        states={
            "a": ["PENDING", "RUNNING", "SUCCEEDED"],
            "b": ["RUNNING", "SUCCEEDED"],
        }
    )
    clock = _wait(client, ["a", "b"])
    assert clock.now == 600


def test_wait_with_nothing_to_wait_on():
    assert _wait(FakeGeeClient(), []).now == 0


def test_wait_raises_on_failure():
    client = FakeGeeClient(states={"a": ["RUNNING", "FAILED"], "b": ["RUNNING"]})
    with pytest.raises(ExportError, match="a FAILED \\(boom\\)"):
        _wait(client, ["a", "b"])


def test_wait_raises_on_cancel():
    client = FakeGeeClient(states={"a": ["CANCELLED"]})
    with pytest.raises(ExportError, match="a CANCELLED"):
        _wait(client, ["a"])


def test_wait_raises_on_timeout():
    client = FakeGeeClient(states={"a": ["SUCCEEDED"], "b": ["RUNNING"]})
    with pytest.raises(ExportError, match="unfinished after 900s: b RUNNING"):
        _wait(client, ["a", "b"], timeout_seconds=900)
