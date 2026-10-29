import hashlib
import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Optional

import boto3
import pandas as pd
import pyarrow.dataset as ds
from botocore.exceptions import ClientError
from pyarrow import fs as pafs

from pipelines.globals import ANALYTICS_BUCKET
from pipelines.utils import parse_s3_uri, s3_uri_exists
from pipelines.zeno_api import Subscription

INTEGRATED_ALERTS_PREFIX = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/integrated-alerts"
GADM_AOIS_URI = f"s3://{ANALYTICS_BUCKET}/vectors/aois/gadm/v4.1.85/aois.parquet"

# The email only counts high and highest confidence alerts.
EMAIL_CONFIDENCES = ["high", "highest"]
# SBTN natural lands classes (natural_lands_class in the admin parquet). Natural
# forests classes are natural forests, mangroves, wetland natural forests and natural
# peat forests. Natural lands classes are natural forests classes plus short
# vegetation, water, mangroves, bare, snow, and wetland/peat forests and short
# vegetation;
NATURAL_LANDS_CLASSES = list(range(2, 12))
NATURAL_FOREST_CLASSES = [2, 5, 8, 9]
# Dashboards shown in the email's table; the rest are only counted.
MAX_DASHBOARDS = 10
# Each email covers a fixed Wednesday to Tuesday week.
WEEK_DAYS = 7
WEEK_END_WEEKDAY = 1  # Tuesday
# A week is only emailed once the integrated alerts version is at least this
# many days after its Tuesday, since alerts reach the data a few days after
# their alert date. (The version is usually Sunday's, 5 days after.)
MIN_DAYS_AFTER_WEEK = 4
# The last week emailed, and a marker for each email sent, live here.
SUBSCRIPTION_EMAILS_PREFIX = f"{INTEGRATED_ALERTS_PREFIX}/subscription-emails"
LAST_SENT_WEEK_URI = f"{SUBSCRIPTION_EMAILS_PREFIX}/last_sent_week_end"


def read_latest_integrated_alerts_version() -> str:
    bucket, key = parse_s3_uri(f"{INTEGRATED_ALERTS_PREFIX}/latest")
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
    return body.read().decode("utf-8").strip()


def version_date(version: str) -> date:
    """The date of an integrated alerts version like v20261025."""
    return datetime.strptime(version, "v%Y%m%d").date()


def latest_ready_week_end(alerts_date: date) -> date:
    """The last Tuesday at least MIN_DAYS_AFTER_WEEK days before alerts_date (the
    integrated alerts version date)."""
    last_day = alerts_date - timedelta(days=MIN_DAYS_AFTER_WEEK)
    return last_day - timedelta(days=(last_day.weekday() - WEEK_END_WEEKDAY) % 7)


def next_week_end(
    last_sent_week_end: Optional[date], alerts_date: date
) -> Optional[date]:
    """The Tuesday ending the next week to email, or None if that week isn't
    ready yet in the integrated alerts version of alerts_date.

    The next week is always the one after the last week sent (or, on the first
    run, the latest ready week), so consecutive emails cover consecutive weeks
    with no gaps or overlaps, even if a run is late or a week's update fails.

    Each alert is meant to be emailed once, in the week of its alert date. That
    assumes all of a week's high and highest confidence alerts are in the data by
    the time the week is emailed. An alert that reaches the data later with an
    earlier alert date (integrated alerts merge sources with different delays),
    or that is only raised to high confidence later, is never emailed, since its
    week was already sent. MIN_DAYS_AFTER_WEEK is meant to make this rare.
    """
    ready_week_end = latest_ready_week_end(alerts_date)
    if last_sent_week_end is None:
        return ready_week_end
    week_end = last_sent_week_end + timedelta(days=WEEK_DAYS)
    return week_end if week_end <= ready_week_end else None


def read_last_sent_week_end() -> Optional[date]:
    bucket, key = parse_s3_uri(LAST_SENT_WEEK_URI)
    try:
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    return date.fromisoformat(body.read().decode("utf-8").strip())


def write_last_sent_week_end(week_end: date) -> None:
    bucket, key = parse_s3_uri(LAST_SENT_WEEK_URI)
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=week_end.isoformat().encode("utf-8"),
        ContentType="text/plain",
    )


def week_range(end_date: date) -> tuple[date, date]:
    """The first and last days (inclusive) of the week ending on end_date."""
    return end_date - timedelta(days=WEEK_DAYS - 1), end_date


def email_subscriptions(
    subscriptions: list[Subscription],
) -> tuple[list[Subscription], list[Subscription]]:
    """Split subscriptions into those the email handles (integrated alerts on
    admin AOIs) and the rest."""
    handled, skipped = [], []
    for subscription in subscriptions:
        if (
            subscription.dataset == "integrated_alerts"
            and subscription.aoi_type == "admin"
        ):
            handled.append(subscription)
        else:
            skipped.append(subscription)
    return handled, skipped


def _s3_dataset(uri: str) -> ds.Dataset:
    # Open the uri in pyarrow's own S3 filesystem, so pyarrow's to_table call can
    # read just the matching rows based on the filter spec.
    bucket, key = parse_s3_uri(uri)
    return ds.dataset(
        f"{bucket}/{key}", filesystem=pafs.S3FileSystem(region="us-east-1")
    )


def read_alert_areas(
    admin_results_uri: str, aoi_ids: list[str], start_date: date, end_date: date
) -> pd.DataFrame:
    """High and highest confidence alert areas (ha) of each AOI from start_date
    to end_date, as columns aoi_id, total_ha, natural_lands_ha and
    natural_forest_ha. AOIs without alerts have no row."""
    table = _s3_dataset(admin_results_uri).to_table(
        columns=["aoi_id", "natural_lands_class", "area_ha"],
        filter=(
            ds.field("aoi_id").isin(aoi_ids)
            & (ds.field("alert_date") >= start_date)
            & (ds.field("alert_date") <= end_date)
            & ds.field("alert_confidence").isin(EMAIL_CONFIDENCES)
        ),
    )
    return summarize_alert_areas(table.to_pandas())


def summarize_alert_areas(alerts: pd.DataFrame) -> pd.DataFrame:
    """Sum rows of aoi_id, natural_lands_class and area_ha into total, natural
    lands and natural forest areas of each AOI."""
    land_class = alerts["natural_lands_class"]
    areas = pd.DataFrame(
        {
            "aoi_id": alerts["aoi_id"],
            "total_ha": alerts["area_ha"],
            "natural_lands_ha": alerts["area_ha"].where(
                land_class.isin(NATURAL_LANDS_CLASSES), 0.0
            ),
            "natural_forest_ha": alerts["area_ha"].where(
                land_class.isin(NATURAL_FOREST_CLASSES), 0.0
            ),
        }
    )
    return areas.groupby("aoi_id", as_index=False).sum()


def read_aoi_names(aoi_ids: list[str]) -> dict[str, str]:
    """Display names like "Paraná, Brazil" for GADM aoi_ids."""
    table = _s3_dataset(GADM_AOIS_URI).to_table(
        columns=["aoi_id", "adm_level", "country", "name_1", "name_2"],
        filter=ds.field("aoi_id").isin(aoi_ids),
    )
    return aoi_names(table.to_pandas())


def aoi_names(aois: pd.DataFrame) -> dict[str, str]:
    """Names of GADM AOIs, from the AOI itself up to its country, e.g. "Brazil",
    "Paraná, Brazil" or "Abatiá, Paraná, Brazil"."""
    name_columns = {0: ["country"], 1: ["name_1", "country"]}
    names = {}
    for aoi in aois.itertuples():
        columns = name_columns.get(aoi.adm_level, ["name_2", "name_1", "country"])
        names[aoi.aoi_id] = ", ".join(getattr(aoi, column) for column in columns)
    return names


def format_ha(area_ha: float) -> str:
    """Whole hectares with thousands separators, or "<1" for a non-zero area
    under 1 ha."""
    if 0 < area_ha < 1:
        return "<1"
    return f"{round(area_ha):,}"


def format_date(day: date) -> str:
    return f"{day.day} {day:%b %Y}"


def format_week(start_date: date, end_date: date) -> str:
    """E.g. "22 – 28 Sep 2026", "28 Sep – 4 Oct 2026" or
    "29 Dec 2026 – 4 Jan 2027"."""
    if start_date.year != end_date.year:
        start = format_date(start_date)
    elif start_date.month != end_date.month:
        start = f"{start_date.day} {start_date:%b}"
    else:
        start = str(start_date.day)
    return f"{start} – {format_date(end_date)}"


def _bar_pct(area_ha: float, max_total_ha: float) -> int:
    return round(100 * area_ha / max_total_ha)


def build_emails(
    subscriptions: list[Subscription],
    areas: pd.DataFrame,
    names: dict[str, str],
    start_date: date,
    end_date: date,
    latest_update: date,
) -> dict[str, dict]:
    """The SparkPost substitution_data of each subscriber's email, by email
    address, for the week from start_date to end_date, using the integrated
    alerts version of date latest_update. Each dashboard (with one AOI) with
    alerts in the week is a row; rows are ranked by total area and only the top
    MAX_DASHBOARDS are shown. Subscribers whose dashboards have no alerts get no
    email."""
    areas_by_aoi = areas.set_index("aoi_id")
    dashboards_by_email: dict[str, dict[str, dict]] = defaultdict(dict)
    for subscription in subscriptions:
        if subscription.aoi_id not in areas_by_aoi.index:
            continue
        aoi_areas = areas_by_aoi.loc[subscription.aoi_id]
        if aoi_areas.total_ha <= 0:
            continue
        # Keyed by dashboard, so a dashboard subscribed twice is shown once.
        dashboards_by_email[subscription.user_email][subscription.dashboard_id] = {
            "dashboard_id": subscription.dashboard_id,
            "name": names.get(subscription.aoi_id, subscription.aoi_id),
            "total_ha": float(aoi_areas.total_ha),
            "natural_lands_ha": float(aoi_areas.natural_lands_ha),
            "natural_forest_ha": float(aoi_areas.natural_forest_ha),
        }

    emails = {}
    for email, dashboards in dashboards_by_email.items():
        ranked = sorted(dashboards.values(), key=lambda d: (-d["total_ha"], d["name"]))
        shown = ranked[:MAX_DASHBOARDS]
        max_total_ha = shown[0]["total_ha"]
        emails[email] = {
            "week_label": format_week(start_date, end_date),
            "latest_update": format_date(latest_update),
            "total_ha": format_ha(sum(d["total_ha"] for d in ranked)),
            "dashboard_count": len(ranked),
            "more_dashboard_count": len(ranked) - len(shown),
            "dashboards": [
                {
                    "dashboard_id": d["dashboard_id"],
                    "name": d["name"],
                    "total_ha": format_ha(d["total_ha"]),
                    "natural_lands_ha": format_ha(d["natural_lands_ha"]),
                    "natural_forest_ha": format_ha(d["natural_forest_ha"]),
                    # Bar segment widths, as percents of the full bar width,
                    # which is the largest row's total.
                    "bar_natural_forest_pct": _bar_pct(
                        d["natural_forest_ha"], max_total_ha
                    ),
                    "bar_other_natural_pct": _bar_pct(
                        d["natural_lands_ha"] - d["natural_forest_ha"], max_total_ha
                    ),
                    "bar_outside_pct": _bar_pct(
                        d["total_ha"] - d["natural_lands_ha"], max_total_ha
                    ),
                }
                for d in shown
            ],
        }
    return emails


def sent_marker_uri(week_end: date, email: str) -> str:
    """Where a week's sent email is recorded, so a re-run doesn't send it again.
    Keyed by a hash, to keep email addresses out of S3 keys."""
    email_hash = hashlib.sha256(email.encode("utf-8")).hexdigest()[:16]
    return f"{SUBSCRIPTION_EMAILS_PREFIX}/{week_end.isoformat()}/{email_hash}.json"


def was_sent(week_end: date, email: str) -> bool:
    return s3_uri_exists(sent_marker_uri(week_end, email))


def mark_sent(week_end: date, email: str, results: dict) -> None:
    bucket, key = parse_s3_uri(sent_marker_uri(week_end, email))
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(results).encode("utf-8"),
        ContentType="application/json",
    )
