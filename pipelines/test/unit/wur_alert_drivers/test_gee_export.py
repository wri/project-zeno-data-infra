from datetime import date, timedelta

import numpy as np
import pytest

from pipelines.wur_alert_drivers.gee_export import (
    ExportError,
    encode_driver_alerts,
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


class FakeImage:
    """numpy-backed stand-in for the ee.Image methods encode_driver_alerts uses,
    so the (server-side) date arithmetic can be checked locally."""

    def __init__(self, bands):
        self.bands = {k: np.asarray(v) for k, v in bands.items()}

    def _value(self):
        (value,) = self.bands.values()
        return value

    def _new(self, value):
        return FakeImage({"value": value})

    @staticmethod
    def _operand(other):
        return other._value() if isinstance(other, FakeImage) else other

    def select(self, name):
        return FakeImage({name: self.bands[name]})

    def add(self, other):
        return self._new(self._value() + self._operand(other))

    def subtract(self, other):
        return self._new(self._value() - self._operand(other))

    def multiply(self, other):
        return self._new(self._value() * self._operand(other))

    def divide(self, other):
        # Like ee, divide is floating-point division.
        return self._new(self._value() / self._operand(other))

    def floor(self):
        return self._new(np.floor(self._value()))

    def gt(self, other):
        return self._new((self._value() > self._operand(other)).astype(int))

    def toInt32(self):
        return self._new(self._value().astype(np.int32))

    def rename(self, name):
        return FakeImage({name: self._value()})


# Downstream code decodes the date part as days since this date.
ALERT_EPOCH = date(2014, 12, 31)

# Year starts and ends, and the days around leap days, from the first year of
# WUR's historical data through years after the current latest data.
ENCODING_DATES = [
    date(2015, 1, 1),
    date(2016, 12, 31),
    date(2022, 1, 1),
    date(2023, 12, 31),
    date(2024, 1, 1),
    date(2024, 2, 28),
    date(2024, 2, 29),
    date(2024, 3, 1),
    date(2024, 12, 31),
    date(2025, 1, 1),
    date(2025, 12, 31),
    date(2026, 1, 1),
    date(2026, 10, 31),
    date(2027, 1, 1),
    date(2028, 12, 31),
    date(2029, 1, 1),
    date(2049, 12, 31),
]


def _yyddd(d):
    return (d.year - 2000) * 1000 + d.timetuple().tm_yday


def _encode(classes, dates):
    image = FakeImage(
        {"class": np.array(classes), "date": np.array([_yyddd(d) for d in dates])}
    )
    encoded = encode_driver_alerts(image)
    assert list(encoded.bands) == ["alert_drivers_encoded"]
    assert encoded.bands["alert_drivers_encoded"].dtype == np.int32
    return encoded.bands["alert_drivers_encoded"].tolist()


def test_encoded_dates_decode_to_the_original_dates():
    encoded = _encode([1] * len(ENCODING_DATES), ENCODING_DATES)
    decoded = [ALERT_EPOCH + timedelta(days=v % 100000) for v in encoded]
    assert decoded == ENCODING_DATES


def test_encoding_known_values():
    # 2015-01-01 is day 1, and 2026-01-01 is 11 years (3 of them leap years:
    # 2016, 2020, 2024) after that: 1 + 11 * 365 + 3 = 4019.
    assert _encode([1, 5], [date(2015, 1, 1), date(2026, 1, 1)]) == [
        100001,
        504019,
    ]


def test_consecutive_days_encode_to_consecutive_values():
    # Every day 2022-2030 is one more than the day before, so no day is skipped
    # or repeated at a year boundary or leap day.
    start = date(2022, 1, 1)
    dates = [start + timedelta(days=i) for i in range(9 * 366)]
    encoded = _encode([3] * len(dates), dates)
    assert np.all(np.diff(encoded) == 1)
    assert encoded[0] == 300000 + (start - ALERT_EPOCH).days
