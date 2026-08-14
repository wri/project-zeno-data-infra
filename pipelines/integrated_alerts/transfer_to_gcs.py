import json
import os
import subprocess
import tempfile
import time

from pipelines.integrated_alerts.register_gee_asset import PROJECT_CONFIG
from pipelines.utils import get_secret

# Quick metadata calls (create, describe) should return almost immediately.
QUICK_TIMEOUT = 60

TERMINAL_STATUSES = {"SUCCESS", "FAILED", "ABORTED"}


def _run_gcloud(args, env, timeout=None, check=True):
    result = subprocess.run(
        ["gcloud", *args], capture_output=True, text=True, timeout=timeout, env=env
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"gcloud {' '.join(args)} failed (exit {result.returncode}): {result.stderr}"
        )
    return result


def _get_latest_operation_name(job_name, project_flag, env):
    """The name of the job's most recent run.

    A transfer *job* is a reusable, schedulable config -- `jobs describe`'s
    own `status` field (ENABLED/DISABLED/DELETED) only reflects whether
    that config is active, never whether a given run succeeded. Each actual
    execution is a separate *operation* resource, with its own status
    (IN_PROGRESS/SUCCESS/FAILED/ABORTED) and progress counters -- that's
    what actually needs polling. `latestOperationName` may briefly be
    absent right after job creation, before the job's first (and, for a
    one-time job, only) run has started.
    """
    job = json.loads(
        _run_gcloud(
            ["transfer", "jobs", "describe", job_name, project_flag, "--format=json"],
            env,
            timeout=QUICK_TIMEOUT,
        ).stdout
    )
    return job.get("latestOperationName")


def _get_operation(operation_name, project_flag, env):
    return json.loads(
        _run_gcloud(
            [
                "transfer", "operations", "describe", operation_name,
                project_flag, "--format=json",
            ],
            env,
            timeout=QUICK_TIMEOUT,
        ).stdout
    )


def transfer_s3_to_gcs(
    source_uri,
    destination_uri,
    source_creds_secret_id,
    project,
    timeout,
    include_prefixes=None,
    poll_interval=60,
) -> str:
    """Copy objects from an S3 uri to a GCS uri via a one-time Storage
    Transfer Service job, and block until the transfer completes.

    Authenticates as `project`'s service account (see PROJECT_CONFIG in
    register_gee_asset.py) so that account's permissions are used for the
    GCS destination; source_creds_secret_id is the AWS Secrets Manager
    secret (same JSON form as gcloud's own AwsAccessKey format) holding the
    separate AWS credentials Storage Transfer Service needs to read the S3
    source.

    Polls `gcloud transfer operations describe` every poll_interval
    seconds, printing status/progress each time, until the operation
    reaches a terminal status or timeout seconds have elapsed -- there's no
    non-blocking "check once" mode in gcloud's own `jobs monitor` command
    (it only offers a real-time, blocking progress display), so this polls
    the same underlying resource directly instead.

    This is a one-time (non-recurring) job, so it only ever has a single
    operation (Storage Transfer's per-run resource, distinct from the job
    itself -- see `_get_latest_operation_name`'s docstring). Its name is
    looked up once, waiting only if the job hasn't started running yet;
    once found, the name is fixed for the rest of this call, so the actual
    polling loop below only re-checks that one operation's status.

    Authentication is via the CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE
    environment variable, scoped to each `gcloud` subprocess call's own
    environment only -- unlike `gcloud auth activate-service-account`, this
    never touches gcloud's persistent config, so there's no active-account
    state to capture or restore. That variable and `--source-creds-file`
    both need an actual file (not raw JSON), so both secrets -- fetched
    from AWS Secrets Manager, not read from local files -- are each written
    to their own temporary file for the duration of this call only, then
    removed.
    """
    config = PROJECT_CONFIG[project]
    project_flag = f"--project={project}"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as cred_file, \
            tempfile.NamedTemporaryFile(mode="w", suffix=".json") as source_cred_file:
        cred_file.write(get_secret(config["secret_id"]))
        cred_file.flush()
        source_cred_file.write(get_secret(source_creds_secret_id))
        source_cred_file.flush()
        env = {
            **os.environ,
            "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE": cred_file.name,
        }

        create_args = [
            "transfer", "jobs", "create", source_uri, destination_uri,
            f"--source-creds-file={source_cred_file.name}",
            project_flag,
            "--format=value(name)",
        ]
        if include_prefixes:
            create_args.append(f"--include-prefixes={','.join(include_prefixes)}")

        job_name = _run_gcloud(create_args, env, timeout=QUICK_TIMEOUT).stdout.strip()

        deadline = time.monotonic() + timeout

        # A one-time job's operation name is fixed for its whole run, but may
        # not exist for a brief moment right after creation, before the job's
        # single run has started.
        operation_name = _get_latest_operation_name(job_name, project_flag, env)
        while not operation_name:
            print(f"Transfer job {job_name}: waiting for it to start running")
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Transfer job {job_name} did not start running within {timeout}s")
            time.sleep(poll_interval)
            operation_name = _get_latest_operation_name(job_name, project_flag, env)

        # The actual polling: operation_name never changes from here on, so
        # only its status/progress needs re-checking.
        status = None
        while True:
            operation = _get_operation(operation_name, project_flag, env)
            metadata = operation.get("metadata", {})
            status = operation.get("status") or metadata.get("status")
            print(
                f"Transfer job {job_name}: status={status} "
                f"counters={metadata.get('counters')}"
            )
            if status in TERMINAL_STATUSES:
                break

            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Transfer job {job_name} did not finish within {timeout}s "
                    f"(last status={status!r})"
                )
            time.sleep(poll_interval)

    if status != "SUCCESS":
        raise RuntimeError(
            f"Transfer job {job_name} (operation {operation_name}) did not "
            f"succeed: status={status!r}"
        )

    return job_name
