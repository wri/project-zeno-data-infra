import os
from unittest.mock import MagicMock, patch

import pytest

from pipelines.integrated_alerts.transfer_to_gcs import transfer_s3_to_gcs


def _ok(stdout=""):
    return MagicMock(returncode=0, stdout=stdout, stderr="")


@pytest.fixture
def mock_run():
    with patch("pipelines.integrated_alerts.transfer_to_gcs.subprocess.run") as mock_run:
        yield mock_run


@pytest.fixture
def mock_sleep():
    with patch("pipelines.integrated_alerts.transfer_to_gcs.time.sleep") as mock_sleep:
        yield mock_sleep


@pytest.fixture
def mock_get_secret():
    with patch("pipelines.integrated_alerts.transfer_to_gcs.get_secret") as mock_get_secret:
        mock_get_secret.side_effect = lambda secret_id: f"secret-content-for-{secret_id}"
        yield mock_get_secret


def _find_source_creds_file(create_args):
    prefix = "--source-creds-file="
    return next(arg[len(prefix):] for arg in create_args if arg.startswith(prefix))


def test_transfer_s3_to_gcs_happy_path(mock_run, mock_sleep, mock_get_secret):
    mock_run.side_effect = [
        _ok("transferJobs/12345\n"),  # jobs create
        _ok('{"latestOperationName": "transferOperations/98765"}'),  # jobs describe (poll 1)
        _ok('{"status": "SUCCESS", "metadata": {"counters": {"bytesCopiedToSink": "10"}}}'),  # operations describe (poll 1)
    ]

    job_name = transfer_s3_to_gcs(
        "s3://gfw-data-lake/gfw_integrated_dist_alerts/v20250105/raster/epsg-4326/cog/",
        "gs://wri-lcl-integrated-alerts/",
        "s3-auth/gfw-data-lake-readonly",
        project="landandcarbon",
        timeout=3600,
        include_prefixes=["intdist_tropics.tif"],
    )

    assert job_name == "transferJobs/12345"
    mock_sleep.assert_not_called()  # terminal status reached on the first poll
    mock_get_secret.assert_any_call("gcs-auth/landandcarbon")
    mock_get_secret.assert_any_call("s3-auth/gfw-data-lake-readonly")

    create_call = mock_run.call_args_list[0]
    create_args = create_call.args[0]
    assert create_args[:5] == [
        "gcloud",
        "transfer",
        "jobs",
        "create",
        "s3://gfw-data-lake/gfw_integrated_dist_alerts/v20250105/raster/epsg-4326/cog/",
    ]
    assert "gs://wri-lcl-integrated-alerts/" in create_args
    assert "--project=landandcarbon" in create_args
    assert "--include-prefixes=intdist_tropics.tif" in create_args

    # No --account/activate-service-account calls at all: auth is via the
    # CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE env var passed to every call,
    # pointing at the same temp file (holding the fetched secret) each time.
    assert not any("--account" in arg for arg in create_args)
    cred_paths = {
        call.kwargs["env"]["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"]
        for call in mock_run.call_args_list
    }
    assert len(cred_paths) == 1

    # --source-creds-file points at a separate temp file from the
    # destination-project credential.
    source_creds_path = _find_source_creds_file(create_args)
    assert source_creds_path not in cred_paths

    describe_call = mock_run.call_args_list[1]
    assert describe_call.args[0][:4] == ["gcloud", "transfer", "jobs", "describe"]
    assert "transferJobs/12345" in describe_call.args[0]

    op_describe_call = mock_run.call_args_list[2]
    assert op_describe_call.args[0][:4] == ["gcloud", "transfer", "operations", "describe"]
    assert "transferOperations/98765" in op_describe_call.args[0]


def test_transfer_s3_to_gcs_writes_secrets_to_temp_files_then_cleans_up(
    mock_run, mock_sleep, mock_get_secret
):
    """Both the destination-project credential and the source S3 credential
    are fetched from Secrets Manager and written to their own real temp
    file (not passed as raw JSON), and both files are removed once the
    function returns."""
    written = {}

    def fake_run(args, **kwargs):
        if "dest_cred_path" not in written:
            dest_path = kwargs["env"]["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"]
            written["dest_cred_path"] = dest_path
            with open(dest_path) as f:
                written["dest_cred_contents"] = f.read()
        if args[:4] == ["gcloud", "transfer", "jobs", "create"]:
            source_creds_path = _find_source_creds_file(args)
            written["source_creds_path"] = source_creds_path
            with open(source_creds_path) as f:
                written["source_creds_contents"] = f.read()
            return _ok("transferJobs/12345\n")
        if args[:4] == ["gcloud", "transfer", "jobs", "describe"]:
            return _ok('{"latestOperationName": "transferOperations/98765"}')
        return _ok('{"status": "SUCCESS", "metadata": {"counters": {}}}')

    mock_run.side_effect = fake_run

    transfer_s3_to_gcs(
        "s3://bucket/prefix/",
        "gs://dest/",
        "s3-auth/gfw-data-lake-readonly",
        project="landandcarbon",
        timeout=3600,
    )

    assert written["dest_cred_contents"] == "secret-content-for-gcs-auth/landandcarbon"
    assert written["source_creds_contents"] == (
        "secret-content-for-s3-auth/gfw-data-lake-readonly"
    )
    assert not os.path.exists(written["dest_cred_path"])  # cleaned up after the call
    assert not os.path.exists(written["source_creds_path"])


def test_transfer_s3_to_gcs_waits_for_operation_to_start(mock_run, mock_sleep, mock_get_secret):
    """Right after creation, latestOperationName may briefly be absent; that
    part polls jobs describe. Once found, only operations describe (not
    jobs describe again) is polled for status/progress."""
    mock_run.side_effect = [
        _ok("transferJobs/12345\n"),  # jobs create
        _ok("{}"),  # jobs describe: no latestOperationName yet
        _ok('{"latestOperationName": "transferOperations/98765"}'),  # jobs describe: found
        _ok('{"status": "IN_PROGRESS", "metadata": {"counters": {}}}'),  # operations describe
        _ok('{"status": "SUCCESS", "metadata": {"counters": {}}}'),  # operations describe
    ]

    job_name = transfer_s3_to_gcs(
        "s3://bucket/prefix/",
        "gs://dest/",
        "s3-auth/gfw-data-lake-readonly",
        project="landandcarbon",
        timeout=3600,
    )

    assert job_name == "transferJobs/12345"
    assert mock_sleep.call_count == 2  # after "no op yet" and after "IN_PROGRESS"
    mock_sleep.assert_called_with(60)

    describe_calls = [
        call for call in mock_run.call_args_list
        if call.args[0][:4] == ["gcloud", "transfer", "jobs", "describe"]
    ]
    operation_describe_calls = [
        call for call in mock_run.call_args_list
        if call.args[0][:4] == ["gcloud", "transfer", "operations", "describe"]
    ]
    # jobs describe is only polled until the operation name is found (twice
    # here); after that, only operations describe is polled for status.
    assert len(describe_calls) == 2
    assert len(operation_describe_calls) == 2


def test_transfer_s3_to_gcs_uses_forma_250_secret(mock_run, mock_sleep, mock_get_secret):
    mock_run.side_effect = [
        _ok("transferJobs/12345\n"),  # jobs create
        _ok('{"latestOperationName": "transferOperations/98765"}'),  # jobs describe
        _ok('{"status": "SUCCESS", "metadata": {"counters": {}}}'),  # operations describe
    ]

    transfer_s3_to_gcs(
        "s3://bucket/prefix/",
        "gs://dest/",
        "s3-auth/gfw-data-lake-readonly",
        project="forma-250",
        timeout=3600,
    )

    create_call = mock_run.call_args_list[0]
    assert "--project=forma-250" in create_call.args[0]
    mock_get_secret.assert_any_call("gcs-auth/forma-250")


def test_transfer_s3_to_gcs_raises_when_create_fails(mock_run, mock_sleep, mock_get_secret):
    mock_run.side_effect = [
        MagicMock(returncode=1, stdout="", stderr="permission denied"),  # jobs create
    ]

    with pytest.raises(RuntimeError, match="permission denied"):
        transfer_s3_to_gcs(
            "s3://bucket/prefix/",
            "gs://dest/",
            "s3-auth/gfw-data-lake-readonly",
            project="landandcarbon",
            timeout=3600,
        )


def test_transfer_s3_to_gcs_raises_when_operation_not_successful(mock_run, mock_sleep, mock_get_secret):
    mock_run.side_effect = [
        _ok("transferJobs/12345\n"),  # jobs create
        _ok('{"latestOperationName": "transferOperations/98765"}'),  # jobs describe
        _ok('{"status": "FAILED", "metadata": {"counters": {}}}'),  # operations describe
    ]

    with pytest.raises(RuntimeError, match="FAILED"):
        transfer_s3_to_gcs(
            "s3://bucket/prefix/",
            "gs://dest/",
            "s3-auth/gfw-data-lake-readonly",
            project="landandcarbon",
            timeout=3600,
        )


def test_transfer_s3_to_gcs_raises_on_timeout(mock_run, mock_sleep, mock_get_secret):
    """If the operation never reaches a terminal status before the timeout
    budget is exhausted, the function raises rather than polling forever."""
    with patch("pipelines.integrated_alerts.transfer_to_gcs.time.monotonic") as mock_monotonic:
        # First call establishes the deadline; every call after that looks
        # like it's already past it, so the loop raises on its first pass.
        mock_monotonic.side_effect = [0, 100]

        mock_run.side_effect = [
            _ok("transferJobs/12345\n"),  # jobs create
            _ok('{"latestOperationName": "transferOperations/98765"}'),  # jobs describe
            _ok('{"status": "IN_PROGRESS", "metadata": {"counters": {}}}'),  # operations describe
        ]

        with pytest.raises(RuntimeError, match="did not finish within"):
            transfer_s3_to_gcs(
                "s3://bucket/prefix/",
                "gs://dest/",
                "s3-auth/gfw-data-lake-readonly",
                project="landandcarbon",
                timeout=10,
            )

    mock_sleep.assert_not_called()
