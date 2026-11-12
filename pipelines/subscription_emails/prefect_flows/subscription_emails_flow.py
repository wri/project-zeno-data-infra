import json
from collections import Counter
from datetime import date
from typing import Optional

from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.sparkpost import (
    RejectedRecipientError,
    SparkPostClient,
    is_transient_error,
)
from pipelines.subscription_emails import stages
from pipelines.utils import get_secret
from pipelines.zeno_api import Subscription, ZenoApiClient

SPARKPOST_API_KEY_SECRET_ID = "sparkpost/api-key"
ZENO_SUPERUSER_KEY_SECRET_ID = "project-zeno/superuser-key"
# The SparkPost template's HTML is kept in ../templates/gnw_weekly_alerts.html.
# Its subject (set in SparkPost) is
#   Your weekly alerts: {{total_area}} across {{dashboard_count}}
#   {{if dashboard_count == 1}}dashboard{{else}}dashboards{{end}}
TEMPLATE_ID = "gnw-weekly-alerts"


@task
def list_subscriptions_task() -> list[Subscription]:
    # The key is fetched inside the task, so it's never a task parameter (which
    # Prefect would log and hash for its cache key).
    client = ZenoApiClient(token=get_secret(ZENO_SUPERUSER_KEY_SECRET_ID))
    return client.list_subscriptions()


@task
def read_alert_areas_task(results_uris, aoi_ids, start_date, end_date):
    return stages.read_alert_areas(results_uris, aoi_ids, start_date, end_date)


@task
def read_aoi_names_task(aoi_ids_by_type):
    return stages.read_names(aoi_ids_by_type)


def retry_if_transient(task, task_run, state) -> bool:
    """A Prefect retry condition: only retry a send that may succeed later (see
    is_transient_error), not e.g. one with a bad API key or template."""
    return isinstance(state.data, BaseException) and is_transient_error(state.data)


@task(retries=2, retry_delay_seconds=60, retry_condition_fn=retry_if_transient)
def send_email_task(
    week_end: date,
    email: str,
    aoi_types: list[str],
    substitution_data: dict,
    test_recipient: Optional[str],
) -> str:
    """Send one email of a subscriber's dashboards of these AOI types, and return
    "sent" or "rejected"."""
    logger = get_run_logger()
    client = SparkPostClient(api_key=get_secret(SPARKPOST_API_KEY_SECRET_ID))
    recipient = test_recipient or email
    try:
        results = client.send_template(TEMPLATE_ID, substitution_data, [recipient])
        status = "sent"
        logger.info(f"Sent email, SparkPost transmission {results.get('id')}")
    except RejectedRecipientError as e:
        # Sending again won't help (e.g. an invalid address), so this is recorded
        # like a sent email, so that it doesn't keep the week from completing.
        results = {"rejected": str(e)}
        status = "rejected"
        logger.warning(f"SparkPost rejected recipient {recipient}: {e}")
    # Test sends aren't recorded, so they never stop the real email being sent.
    if test_recipient is None:
        stages.mark_sent(week_end, email, aoi_types, results)
    return status


def email_week(
    version: str,
    alerts_version_date: date,
    week_end: date,
    subscriptions: list[Subscription],
    send_emails: bool,
    test_recipient: Optional[str],
) -> tuple[int, bool]:
    """Email each subscriber the week's dashboards not yet sent to them whose AOI
    type has results in the version. Returns how many emails there were and
    whether any dashboards are left to send once their results exist."""
    logger = get_run_logger()
    start_date, end_date = stages.week_range(week_end)
    logger.info(f"Integrated alerts version {version}, week {start_date}-{end_date}")
    # Test sends aren't recorded, so they ignore what was sent.
    sent_markers = set() if test_recipient else stages.list_sent_markers(week_end)
    unsent = stages.select_unsent_subscriptions(subscriptions, week_end, sent_markers)
    missing_types = stages.list_aoi_types_without_results(
        version, {s.aoi_type for s in unsent}
    )
    subscriptions = [s for s in unsent if s.aoi_type not in missing_types]
    waiting_count = len(unsent) - len(subscriptions)
    if waiting_count:
        logger.warning(
            f"Not yet sending {waiting_count} dashboards of the week ending "
            f"{end_date}: version {version} has no results for "
            f"{sorted(missing_types)}. Later runs will send them once it has."
        )
    emails = {}
    if subscriptions:
        aoi_ids_by_type = stages.group_aoi_ids_by_type(subscriptions)
        results_uris = [
            stages.alert_results_uri(version, aoi_type)
            for aoi_type in sorted(aoi_ids_by_type)
        ]
        aoi_ids = sorted({s.aoi_id for s in subscriptions})
        areas = read_alert_areas_task(results_uris, aoi_ids, start_date, end_date)
        names = read_aoi_names_task(aoi_ids_by_type)
        emails = stages.build_emails(
            subscriptions, areas, names, start_date, end_date, alerts_version_date
        )
    logger.info(f"{len(emails)} emails for {len(subscriptions)} subscriptions")

    if not send_emails:
        for email, substitution_data in emails.items():
            print(f"{email}: {json.dumps(substitution_data, ensure_ascii=False)}")
        return len(emails), waiting_count > 0

    aoi_types_by_email = stages.group_aoi_types_by_email(subscriptions)
    # Send every email even if some fail, then fail the flow if any did.
    futures = {
        email: send_email_task.submit(
            end_date,
            email,
            aoi_types_by_email[email],
            substitution_data,
            test_recipient,
        )
        for email, substitution_data in emails.items()
    }
    for future in futures.values():
        future.wait()
    failed = [email for email, future in futures.items() if future.state.is_failed()]
    if failed:
        raise RuntimeError(f"{len(failed)} of {len(emails)} emails failed to send")
    statuses = Counter(future.result() for future in futures.values())
    logger.info(f"Emails: {dict(statuses)}")
    return len(emails), waiting_count > 0


@flow(
    name="Subscription emails",
    log_prints=True,
    # Safe to retry: emails already sent are recorded and skipped.
    retries=2,
    retry_delay_seconds=600,
    description=(
        "Send each dashboard subscriber a weekly email of the high and highest "
        "confidence integrated alerts in their dashboards' areas"
    ),
)
def subscription_emails_flow(
    send_emails: bool = False, test_recipient: Optional[str] = None
) -> int:
    """Email each subscriber a Wednesday to Tuesday week's integrated alerts in
    the admin and custom AOIs of their subscribed dashboards.

    The week is the one after the last week emailed, once the latest integrated
    alerts version is far enough past it (see stages.next_week_end); otherwise
    nothing is sent. Emails are sent through a SparkPost template (TEMPLATE_ID),
    filled in with the substitution data built by stages.build_emails. Each sent
    email is recorded in S3 per AOI type, so re-running the flow (e.g. after a
    failure) only sends the dashboards not yet sent. When an AOI type has no
    results in the version, the other types' dashboards are still sent; the week
    is recorded as skipped, and later runs send that type's dashboards in a
    separate email once its results exist.

    Args:
        send_emails: Only if True are the emails sent; otherwise each email's
            substitution data is logged instead, and nothing is recorded.
        test_recipient: If set (with send_emails), send every email to this
            address instead of the subscriber's, and don't record them or the
            week as sent.

    Returns:
        The number of emails sent (or that would be sent, without send_emails).
    """
    logger = get_run_logger()

    version = stages.read_latest_integrated_alerts_version()
    alerts_version_date = stages.version_date(version)
    last_sent_week_end = stages.read_last_sent_week_end()
    week_end = stages.next_week_end(last_sent_week_end, alerts_version_date)
    skipped_weeks = stages.read_skipped_weeks()
    if week_end is None:
        logger.info(
            f"The week after {last_sent_week_end} isn't ready yet in integrated "
            f"alerts version {version}"
        )
    elif week_end < stages.latest_ready_week_end(alerts_version_date):
        logger.warning("Later weeks are also ready; they'll be sent by later runs")
    week_ends = skipped_weeks + ([week_end] if week_end is not None else [])
    if not week_ends:
        return 0

    subscriptions, skipped = stages.email_subscriptions(list_subscriptions_task())
    if skipped:
        logger.warning(
            f"Skipping {len(skipped)} subscriptions that aren't integrated alerts "
            "on admin or custom AOIs"
        )
    email_count, still_skipped_weeks = 0, []
    for each_week_end in week_ends:
        count, has_waiting_dashboards = email_week(
            version,
            alerts_version_date,
            each_week_end,
            subscriptions,
            send_emails,
            test_recipient,
        )
        email_count += count
        if has_waiting_dashboards:
            still_skipped_weeks.append(each_week_end)

    if send_emails and test_recipient is None:
        if still_skipped_weeks != skipped_weeks:
            stages.write_skipped_weeks(still_skipped_weeks)
        if week_end is not None:
            stages.write_last_sent_week_end(week_end)
            deleted = stages.delete_old_sent_markers(date.today())
            logger.info(f"Deleted {deleted} sent markers of old weeks")
    return email_count
