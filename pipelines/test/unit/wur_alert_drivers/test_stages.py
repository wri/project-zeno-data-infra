import pytest

from pipelines.wur_alert_drivers.stages import (
    AssetCreationError,
    create_cog_if_needed,
    create_version_if_needed,
    metadata_body,
    wait_for_assets_saved,
)

DATASET = "my_dataset"
VERSION = "v20261018"


class FakeDataApiClient:
    """In-memory Data API. Each get_assets call returns the next entry of
    asset_states (the last one repeats), and create calls are recorded."""

    def __init__(self, version=None, asset_states=None):
        self.version = version
        self.asset_states = asset_states or [[]]
        self.calls = []

    def get_version(self, dataset, version):
        return self.version

    def create_version(self, dataset, version, body):
        self.calls.append(("create_version", body))

    def get_assets(self, dataset, version):
        states = self.asset_states
        return states.pop(0) if len(states) > 1 else states[0]

    def create_asset(self, dataset, version, body):
        self.calls.append(("create_asset", body))

    def update_version(self, dataset, version, body):
        self.calls.append(("update_version", body))


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def _asset(asset_type, status, asset_id="id"):
    return {"asset_id": asset_id, "asset_type": asset_type, "status": status}


def _wait(client, timeout_seconds=3600):
    clock = FakeClock()
    return wait_for_assets_saved(
        client, DATASET, VERSION, timeout_seconds, sleep=clock.sleep, clock=clock
    )


def test_wait_returns_once_all_assets_are_saved():
    client = FakeDataApiClient(
        asset_states=[
            [_asset("Raster tile set", "pending")],
            [_asset("Raster tile set", "saved"), _asset("COG", "pending")],
            [_asset("Raster tile set", "saved"), _asset("COG", "saved")],
        ]
    )

    assets = _wait(client)

    assert [a["status"] for a in assets] == ["saved", "saved"]


def test_wait_raises_if_an_asset_failed():
    client = FakeDataApiClient(asset_states=[[_asset("Raster tile set", "failed")]])

    with pytest.raises(AssetCreationError, match="failed"):
        _wait(client)


def test_wait_raises_after_timeout():
    client = FakeDataApiClient(asset_states=[[_asset("Raster tile set", "pending")]])

    with pytest.raises(AssetCreationError, match="still pending"):
        _wait(client, timeout_seconds=180)


def test_create_version_skips_creation_if_version_exists():
    # resuming a failed run: the version was already created
    client = FakeDataApiClient(
        version={"version": VERSION},
        asset_states=[[_asset("Raster tile set", "saved")]],
    )

    create_version_if_needed(
        client, DATASET, VERSION, "gs://bucket/source", 3600, sleep=lambda s: None
    )

    assert client.calls == []


def test_create_cog_uses_the_versions_raster_tile_set_as_source():
    client = FakeDataApiClient(
        asset_states=[
            [_asset("Raster tile set", "saved", asset_id="tile-set-id")],
            [
                _asset("Raster tile set", "saved", asset_id="tile-set-id"),
                _asset("COG", "saved"),
            ],
        ]
    )

    create_cog_if_needed(client, DATASET, VERSION, 3600, sleep=lambda s: None)

    ((call, body),) = client.calls
    assert call == "create_asset"
    assert body["asset_type"] == "COG"
    assert body["creation_options"]["source_asset_id"] == "tile-set-id"


def test_create_cog_skips_creation_if_cog_exists():
    client = FakeDataApiClient(
        asset_states=[[_asset("Raster tile set", "saved"), _asset("COG", "saved")]]
    )

    create_cog_if_needed(client, DATASET, VERSION, 3600, sleep=lambda s: None)

    assert client.calls == []


def test_metadata_body_uses_content_date():
    metadata = metadata_body("2026-09-30")["metadata"]

    assert metadata["content_date"] == "2026-09-30"
    assert metadata["last_update"] == "2026-09-30"
    assert metadata["content_date_range"] == {
        "start_date": "2014-12-31",
        "end_date": "2026-09-30",
    }
