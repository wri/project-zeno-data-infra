from datetime import date

import httpx
import pandas as pd
import pytest
from prefect.logging import disable_run_logger
from prefect.states import Failed
from prefect.testing.utilities import prefect_test_harness

from pipelines.sparkpost import RejectedRecipientError
from pipelines.subscription_emails.prefect_flows import subscription_emails_flow
from pipelines.subscription_emails.prefect_flows.subscription_emails_flow import (
    retry_if_transient,
    send_email_task,
)
from pipelines.zeno_api import Subscription

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


@pytest.fixture(scope="module")
def prefect_server():
    with prefect_test_harness():
        yield


def _subscription(email, aoi_type, aoi_id):
    return Subscription(
        subscription_id=f"sub-{aoi_id}",
        dashboard_id=f"dashboard-{aoi_id}",
        user_id=f"user-{email}",
        user_email=email,
        aoi_type=aoi_type,
        aoi_id=aoi_id,
        dataset="integrated_alerts",
    )


def test_subscribers_missing_results_skip_the_week_and_the_rest_are_emailed(
    prefect_server, monkeypatch, sparkpost, markers
):
    stages = subscription_emails_flow.stages
    subscriptions = [
        _subscription("a@x.org", "admin", "BRA.16"),
        _subscription("a@x.org", "custom", "8f0c-saved"),
        _subscription("b@x.org", "admin", "PER.17"),
    ]
    monkeypatch.setattr(
        subscription_emails_flow.ZenoApiClient,
        "list_subscriptions",
        lambda self: subscriptions,
    )
    monkeypatch.setattr(
        stages, "read_latest_integrated_alerts_version", lambda: "v20261108"
    )
    monkeypatch.setattr(stages, "read_last_sent_week_end", lambda: None)
    # The custom results weren't written for this version.
    monkeypatch.setattr(stages, "s3_uri_exists", lambda uri: "custom" not in uri)
    monkeypatch.setattr(
        stages,
        "read_alert_areas",
        lambda uris, aoi_ids, start, end: pd.DataFrame(
            {
                "aoi_id": aoi_ids,
                "total_ha": 10.0,
                "natural_lands_ha": 5.0,
                "natural_forest_ha": 1.0,
            }
        ),
    )
    monkeypatch.setattr(
        stages, "read_names", lambda by_type: {"BRA.16": "Paraná", "PER.17": "Loreto"}
    )
    weeks_recorded = []
    monkeypatch.setattr(stages, "write_last_sent_week_end", weeks_recorded.append)

    flow = subscription_emails_flow.subscription_emails_flow.with_options(retries=0)
    monkeypatch.setattr(stages, "delete_old_sent_markers", lambda today: 0)

    assert flow(send_emails=True) == 1

    assert sparkpost.sent_to == ["b@x.org"]
    assert len(weeks_recorded) == 1
