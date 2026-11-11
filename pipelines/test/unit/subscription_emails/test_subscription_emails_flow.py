from datetime import date

import httpx
import pytest
from prefect.logging import disable_run_logger
from prefect.states import Failed

from pipelines.sparkpost import RejectedRecipientError
from pipelines.subscription_emails.prefect_flows import subscription_emails_flow
from pipelines.subscription_emails.prefect_flows.subscription_emails_flow import (
    retry_if_transient,
    send_email_task,
)

WEEK_END = date(2026, 10, 20)
DATA = {"total_area": "12 ha"}


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


class FakeSparkPost:
    """Stands in for SparkPostClient: records each send's recipients, and raises
    `error` instead of sending if it's set."""

    def __init__(self):
        self.sent_to = []
        self.error = None

    def __call__(self, api_key):
        return self

    def send_template(self, template_id, substitution_data, recipients):
        if self.error is not None:
            raise self.error
        self.sent_to.extend(recipients)
        return {"id": "123", "total_accepted_recipients": len(recipients)}


@pytest.fixture
def sparkpost(monkeypatch):
    fake = FakeSparkPost()
    monkeypatch.setattr(subscription_emails_flow, "SparkPostClient", fake)
    monkeypatch.setattr(subscription_emails_flow, "get_secret", lambda _: "key")
    return fake


@pytest.fixture
def markers(monkeypatch):
    """The sent markers, by (week_end, email), in place of S3."""
    markers = {}
    stages = subscription_emails_flow.stages
    monkeypatch.setattr(
        stages, "was_sent", lambda week_end, email: (week_end, email) in markers
    )
    monkeypatch.setattr(
        stages,
        "mark_sent",
        lambda week_end, email, results: markers.update({(week_end, email): results}),
    )
    return markers


def _send(email="a@x.org", test_recipient=None):
    # .fn runs the task's function directly, without the Prefect engine.
    with disable_run_logger():
        return send_email_task.fn(WEEK_END, email, DATA, test_recipient)


def test_send_email_task_sends_and_records_the_email(sparkpost, markers):
    assert _send() == "sent"

    assert sparkpost.sent_to == ["a@x.org"]
    assert markers == {
        (WEEK_END, "a@x.org"): {"id": "123", "total_accepted_recipients": 1}
    }


def test_send_email_task_skips_an_email_already_sent(sparkpost, markers):
    markers[(WEEK_END, "a@x.org")] = {"id": "earlier"}

    assert _send() == "already sent"

    assert sparkpost.sent_to == []
    assert markers == {(WEEK_END, "a@x.org"): {"id": "earlier"}}


def test_send_email_task_records_a_rejected_recipient(sparkpost, markers):
    sparkpost.error = RejectedRecipientError("At least one valid recipient")

    assert _send(email="not-an-email") == "rejected"

    assert markers == {
        (WEEK_END, "not-an-email"): {"rejected": "At least one valid recipient"}
    }


def test_send_email_task_sends_to_test_recipient_without_recording(sparkpost, markers):
    # Even after the real email was sent, a test send goes out, and isn't recorded.
    markers[(WEEK_END, "a@x.org")] = {"id": "earlier"}

    assert _send(test_recipient="tester@x.org") == "sent"

    assert sparkpost.sent_to == ["tester@x.org"]
    assert markers == {(WEEK_END, "a@x.org"): {"id": "earlier"}}


def test_send_email_task_raises_other_send_errors_without_recording(sparkpost, markers):
    sparkpost.error = _status_error(503)

    with pytest.raises(httpx.HTTPStatusError):
        _send()

    assert markers == {}
