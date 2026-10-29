import json
from datetime import date
from typing import Optional

from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.sparkpost import SparkPostClient
from pipelines.subscription_emails import stages
from pipelines.utils import get_secret
from pipelines.zeno_api import Subscription, ZenoApiClient

SPARKPOST_API_KEY_SECRET_ID = "sparkpost/api-key"
ZENO_SUPERUSER_KEY_SECRET_ID = "project-zeno/superuser-key"
# The SparkPost template's HTML is kept in ../templates/gnw_weekly_alerts.html.
# Its subject (set in SparkPost) is
#   Your weekly alerts: {{total_ha}} ha across {{dashboard_count}}
#   {{if dashboard_count == 1}}dashboard{{else}}dashboards{{end}}
TEMPLATE_ID = "gnw-weekly-alerts"


@task
def list_subscriptions_task() -> list[Subscription]:
    # The key is fetched inside the task, so it's never a task parameter (which
    # Prefect would log and hash for its cache key).
    client = ZenoApiClient(token=get_secret(ZENO_SUPERUSER_KEY_SECRET_ID))
    return client.list_subscriptions()


@task
def read_alert_areas_task(version, aoi_ids, start_date, end_date):
    admin_results_uri = (
        f"{stages.INTEGRATED_ALERTS_PREFIX}/{version}/admin-integrated-alerts.parquet"
    )
    return stages.read_alert_areas(admin_results_uri, aoi_ids, start_date, end_date)


@task
def read_aoi_names_task(aoi_ids):
    return stages.read_aoi_names(aoi_ids)


@task(retries=2, retry_delay_seconds=60)
def send_email_task(
    week_end: date,
    email: str,
    substitution_data: dict,
    test_recipient: Optional[str],
) -> None:
    logger = get_run_logger()
    # Test sends aren't recorded, so they never stop the real email being sent.
    if test_recipient is None and stages.was_sent(week_end, email):
        logger.info("Already sent this week's email; skipping")
        return
    client = SparkPostClient(api_key=get_secret(SPARKPOST_API_KEY_SECRET_ID))
    results = client.send_template(
        TEMPLATE_ID, substitution_data, [test_recipient or email]
    )
    logger.info(f"Sent email, SparkPost transmission {results.get('id')}")
    if test_recipient is None:
        stages.mark_sent(week_end, email, results)


@flow(
    name="Subscription emails",
    log_prints=True,
    description=(
        "Send each dashboard subscriber a weekly email of the high and highest "
        "confidence integrated alerts in their dashboards' areas"
    ),
)
def subscription_emails_flow(
    dry_run: bool = False, test_recipient: Optional[str] = None
) -> int:
    """Email each subscriber a Wednesday to Tuesday week's integrated alerts in
    the admin AOIs of their subscribed dashboards.

    The week is the one after the last week emailed, once the latest integrated
    alerts version is far enough past it (see stages.next_week_end); otherwise
    nothing is sent. Emails are sent through a SparkPost template (TEMPLATE_ID),
    filled in with the substitution data built by stages.build_emails. Each sent
    email is recorded in S3, so re-running the flow (e.g. after a failure) only
    sends the emails not yet sent, and the week is recorded as emailed once they
    all have been.

    Args:
        dry_run: If True, log each email's substitution data instead of sending.
        test_recipient: If set, send every email to this address instead of the
            subscriber's, and don't record them or the week as sent.

    Returns:
        The number of emails sent (or that would be sent, in a dry run).
    """
    logger = get_run_logger()

    version = stages.read_latest_integrated_alerts_version()
    alerts_version_date = stages.version_date(version)
    last_sent_week_end = stages.read_last_sent_week_end()
    week_end = stages.next_week_end(last_sent_week_end, alerts_version_date)
    if week_end is None:
        logger.info(
            f"The week after {last_sent_week_end} isn't ready yet in integrated "
            f"alerts version {version}; nothing to send"
        )
        return 0
    start_date, end_date = stages.week_range(week_end)
    logger.info(f"Integrated alerts version {version}, week {start_date}-{end_date}")
    if end_date < stages.latest_ready_week_end(alerts_version_date):
        logger.warning("Later weeks are also ready; they'll be sent by later runs")

    subscriptions, skipped = stages.email_subscriptions(list_subscriptions_task())
    if skipped:
        logger.warning(
            f"Skipping {len(skipped)} subscriptions that aren't integrated alerts "
            "on admin AOIs"
        )
    emails = {}
    if subscriptions:
        aoi_ids = sorted({s.aoi_id for s in subscriptions})
        areas = read_alert_areas_task(version, aoi_ids, start_date, end_date)
        names = read_aoi_names_task(aoi_ids)
        emails = stages.build_emails(
            subscriptions, areas, names, start_date, end_date, alerts_version_date
        )
    logger.info(f"{len(emails)} emails for {len(subscriptions)} subscriptions")

    if dry_run:
        for email, substitution_data in emails.items():
            print(f"{email}: {json.dumps(substitution_data, ensure_ascii=False)}")
        return len(emails)

    # Send every email even if some fail, then fail the flow if any did.
    futures = {
        email: send_email_task.submit(
            end_date, email, substitution_data, test_recipient
        )
        for email, substitution_data in emails.items()
    }
    for future in futures.values():
        future.wait()
    failed = [email for email, future in futures.items() if future.state.is_failed()]
    if failed:
        raise RuntimeError(f"{len(failed)} of {len(emails)} emails failed to send")
    if test_recipient is None:
        stages.write_last_sent_week_end(end_date)
    return len(emails)
