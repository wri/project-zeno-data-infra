import json
from unittest.mock import MagicMock, patch

import pytest

from pipelines.integrated_alerts.register_gee_asset import register_gee_asset


@pytest.fixture
def mock_ee():
    with patch("pipelines.integrated_alerts.register_gee_asset.ee") as mock_ee:
        mock_ee.data.getAssetAcl.return_value = {"all_users_can_read": True}
        yield mock_ee


@pytest.fixture
def mock_session():
    with patch(
        "pipelines.integrated_alerts.register_gee_asset.AuthorizedSession"
    ) as mock_session_cls:
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.post.return_value = MagicMock(status_code=200, content=b"{}")
        yield mock_session


@pytest.fixture
def mock_get_secret():
    with patch(
        "pipelines.integrated_alerts.register_gee_asset.get_secret"
    ) as mock_get_secret:
        mock_get_secret.side_effect = lambda secret_id: f"key-data-for-{secret_id}"
        yield mock_get_secret


def test_register_gee_asset_builds_expected_request(mock_ee, mock_session, mock_get_secret):
    result = register_gee_asset(
        "gs://data-api-gee-assets/gfw_integrated_dist_alerts/intdist_tropics.tif",
        "gfw_integrated_dist_alerts/intdist_tropics",
        project="forma-250",
        force=True,
        end_time="2026-08-12T00:00:00.000000000Z",
    )

    assert result == "projects/forma-250/assets/gfw_integrated_dist_alerts/intdist_tropics"

    mock_get_secret.assert_called_once_with("gcs-auth/forma-250")
    mock_ee.ServiceAccountCredentials.assert_called_once_with(
        "gfw-gee-export@forma-250.iam.gserviceaccount.com",
        key_data="key-data-for-gcs-auth/forma-250",
    )
    mock_ee.Initialize.assert_called_once()

    mock_session.post.assert_called_once()
    _, kwargs = mock_session.post.call_args
    assert (
        kwargs["url"]
        == "https://earthengine.googleapis.com/v1alpha/projects/forma-250/image:importExternal"
    )
    request = json.loads(kwargs["data"])
    assert request["overwrite"] is True
    manifest = request["imageManifest"]
    assert manifest["name"] == "projects/forma-250/assets/gfw_integrated_dist_alerts/intdist_tropics"
    assert manifest["uriPrefix"] == "gs://data-api-gee-assets/gfw_integrated_dist_alerts/"
    assert manifest["tilesets"] == [
        {"id": "0", "sources": [{"uris": ["intdist_tropics.tif"]}]}
    ]
    assert manifest["bands"] == [
        {
            "id": "date_conf",
            "tilesetId": "0",
            "pyramidingPolicy": "MODE",
            "missingData": {"values": [0]},
        }
    ]
    assert manifest["startTime"] == "2015-01-01T00:00:00.000000000Z"
    assert manifest["endTime"] == "2026-08-12T00:00:00.000000000Z"

    mock_ee.data.setAssetAcl.assert_called_once_with(
        "projects/forma-250/assets/gfw_integrated_dist_alerts/intdist_tropics",
        {"all_users_can_read": True},
    )


def test_register_gee_asset_landandcarbon_uses_lcl_secret(mock_ee, mock_session, mock_get_secret):
    register_gee_asset(
        "gs://bucket/some.tif",
        "some/asset",
        project="landandcarbon",
        end_time="2026-08-12T00:00:00.000000000Z",
    )

    mock_get_secret.assert_called_once_with("gcs-auth/landandcarbon")
    mock_ee.ServiceAccountCredentials.assert_called_once_with(
        "integrated-alerts@landandcarbon.iam.gserviceaccount.com",
        key_data="key-data-for-gcs-auth/landandcarbon",
    )


def test_register_gee_asset_unknown_project_raises(mock_ee, mock_session, mock_get_secret):
    with pytest.raises(ValueError):
        register_gee_asset("gs://bucket/some.tif", "some/asset", project="bogus")

    mock_session.post.assert_not_called()
    mock_get_secret.assert_not_called()


def test_register_gee_asset_failure_raises(mock_ee, mock_session, mock_get_secret):
    mock_session.post.return_value = MagicMock(
        status_code=400, content=b'{"error": "boom"}'
    )

    with pytest.raises(RuntimeError):
        register_gee_asset(
            "gs://bucket/some.tif",
            "some/asset",
            end_time="2026-08-12T00:00:00.000000000Z",
        )

    mock_ee.data.setAssetAcl.assert_not_called()


def test_register_gee_asset_default_end_time_is_today(mock_ee, mock_session, mock_get_secret):
    with patch(
        "pipelines.integrated_alerts.register_gee_asset.datetime"
    ) as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = (
            "2026-08-12T00:00:00.000000000Z"
        )

        register_gee_asset("gs://bucket/some.tif", "some/asset")

        _, kwargs = mock_session.post.call_args
        manifest = json.loads(kwargs["data"])["imageManifest"]
        assert manifest["endTime"] == "2026-08-12T00:00:00.000000000Z"
