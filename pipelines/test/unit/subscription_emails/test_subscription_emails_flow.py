import httpx
from prefect.states import Failed

from pipelines.subscription_emails.prefect_flows.subscription_emails_flow import (
    retry_if_transient,
)


def _status_error(status):
    request = httpx.Request("POST", "https://api.sparkpost.com/api/v1/transmissions")
    return httpx.HTTPStatusError(
        "error", request=request, response=httpx.Response(status, request=request)
    )


def test_retry_if_transient_retries_server_errors():
    # Prefect passes the task's exception as the data of a Failed state.
    assert retry_if_transient(None, None, Failed(data=_status_error(503)))


def test_retry_if_transient_does_not_retry_request_errors():
    assert not retry_if_transient(None, None, Failed(data=_status_error(401)))
