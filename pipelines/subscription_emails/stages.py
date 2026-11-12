import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Callable, Optional

import boto3
import pandas as pd
import pyarrow.dataset as ds
from botocore.exceptions import ClientError
from pyarrow import fs as pafs
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import In

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
# Links in the email, to all of the user's dashboards and to each one.
# (These may later come with the subscriptions from the API instead.)
DASHBOARDS_URL = "https://horizon.globalnaturewatch.org/dashboards"
DASHBOARD_URL = "https://horizon.globalnaturewatch.org/dashboards/{dashboard_id}"
# Each email covers a fixed Wednesday to Tuesday week.
WEEK_DAYS = 7
WEEK_END_WEEKDAY = 1  # Tuesday
# A week is only emailed once the integrated alerts version is at least this
# many days after its Tuesday, since alerts reach the data a few days after
# their alert date. (The version is usually Sunday's, 5 days after.)
MIN_DAYS_AFTER_WEEK = 4
# The last week emailed, the weeks whose emails some subscribers were skipped
# for, and a marker for each email sent, live here.
SUBSCRIPTION_EMAILS_PREFIX = f"{INTEGRATED_ALERTS_PREFIX}/subscription-emails"
LAST_SENT_WEEK_URI = f"{SUBSCRIPTION_EMAILS_PREFIX}/last_sent_week_end"
SKIPPED_WEEKS_URI = f"{SUBSCRIPTION_EMAILS_PREFIX}/skipped_weeks"
SENT_MARKERS_PREFIX = f"{SUBSCRIPTION_EMAILS_PREFIX}/sent"
# A week's sent markers are only needed while the week is being sent, so they're
# deleted once the week ended this many days ago (keeping them a while for
# debugging).
SENT_MARKER_DAYS = 30


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


def read_skipped_weeks() -> list[date]:
    """Read the ends of the weeks that some subscribers weren't yet emailed,
    because their dashboards' results were missing."""
    bucket, key = parse_s3_uri(SKIPPED_WEEKS_URI)
    try:
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return []
        raise
    return [date.fromisoformat(day) for day in json.loads(body.read())]


def write_skipped_weeks(week_ends: list[date]) -> None:
    bucket, key = parse_s3_uri(SKIPPED_WEEKS_URI)
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps([day.isoformat() for day in week_ends]).encode("utf-8"),
        ContentType="application/json",
    )


def week_range(end_date: date) -> tuple[date, date]:
    """The first and last days (inclusive) of the week ending on end_date."""
    return end_date - timedelta(days=WEEK_DAYS - 1), end_date


def email_subscriptions(
    subscriptions: list[Subscription],
) -> tuple[list[Subscription], list[Subscription]]:
    """Split subscriptions into those the email handles (integrated alerts on
    admin and custom AOIs) and the rest."""
    handled, skipped = [], []
    for subscription in subscriptions:
        if (
            subscription.dataset == "integrated_alerts"
            and subscription.aoi_type in EMAIL_AOI_TYPES
        ):
            handled.append(subscription)
        else:
            skipped.append(subscription)
    return handled, skipped


def group_aoi_ids_by_type(subscriptions: list[Subscription]) -> dict[str, list[str]]:
    """Group the subscribed AOI ids by their AOI type."""
    aoi_ids_by_type: dict[str, set[str]] = defaultdict(set)
    for subscription in subscriptions:
        aoi_ids_by_type[subscription.aoi_type].add(subscription.aoi_id)
    return {aoi_type: sorted(ids) for aoi_type, ids in aoi_ids_by_type.items()}


def group_aoi_types_by_email(
    subscriptions: list[Subscription],
) -> dict[str, list[str]]:
    """Group the AOI types of each subscriber's dashboards by their email."""
    aoi_types_by_email: dict[str, set[str]] = defaultdict(set)
    for subscription in subscriptions:
        aoi_types_by_email[subscription.user_email].add(subscription.aoi_type)
    return {email: sorted(types) for email, types in aoi_types_by_email.items()}


def alert_results_uri(version: str, aoi_type: str) -> str:
    file_name = EMAIL_AOI_TYPES[aoi_type].results_file_name
    return f"{INTEGRATED_ALERTS_PREFIX}/{version}/{file_name}"


def list_aoi_types_without_results(version: str, aoi_types: set[str]) -> set[str]:
    """List the AOI types whose results file is missing from the version, e.g.
    when that type's zonal statistics failed in the weekly update."""
    return {
        aoi_type
        for aoi_type in aoi_types
        if not s3_uri_exists(alert_results_uri(version, aoi_type))
    }


def _s3_dataset(uris: list[str]) -> ds.Dataset:
    # Open the uris in pyarrow's own S3 filesystem, so pyarrow's to_table call can
    # read just the matching rows based on the filter spec.
    paths = ["/".join(parse_s3_uri(uri)) for uri in uris]
    return ds.dataset(paths, filesystem=pafs.S3FileSystem(region="us-east-1"))


def read_alert_areas(
    results_uris: list[str], aoi_ids: list[str], start_date: date, end_date: date
) -> pd.DataFrame:
    """High and highest confidence alert areas (ha) of each AOI from start_date
    to end_date, as columns aoi_id, total_ha, natural_lands_ha and
    natural_forest_ha, read from all the results files in one scan. AOIs without
    alerts have no row."""
    table = _s3_dataset(results_uris).to_table(
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


def read_admin_aoi_names(aoi_ids: list[str]) -> dict[str, str]:
    """Display names like "Paraná, Brazil" for GADM aoi_ids."""
    table = _s3_dataset([GADM_AOIS_URI]).to_table(
        columns=["aoi_id", "adm_level", "country", "name_1", "name_2"],
        filter=ds.field("aoi_id").isin(aoi_ids),
    )
    return admin_aoi_names(table.to_pandas())


def read_custom_aoi_names(aoi_ids: list[str]) -> dict[str, str]:
    """Read the names users gave their custom AOIs, as currently saved by the
    API."""
    table = load_catalog("aois").load_table("custom.custom_aois")
    rows = table.scan(
        row_filter=In("aoi_id", aoi_ids), selected_fields=("aoi_id", "name")
    ).to_arrow()
    return dict(zip(rows.column("aoi_id").to_pylist(), rows.column("name").to_pylist()))


@dataclass(frozen=True)
class EmailAoiType:
    """Where the email reads one AOI type's alert areas and names."""

    results_file_name: str  # in each integrated alerts version's folder
    read_names: Callable[[list[str]], dict[str, str]]


# The AOI types the email covers. Their results files share one schema.
EMAIL_AOI_TYPES = {
    "admin": EmailAoiType("admin-integrated-alerts.parquet", read_admin_aoi_names),
    "custom": EmailAoiType("custom-integrated-alerts.parquet", read_custom_aoi_names),
}


def read_names(aoi_ids_by_type: dict[str, list[str]]) -> dict[str, str]:
    """Read the display names of these AOIs, each from its type's name source."""
    names: dict[str, str] = {}
    for aoi_type, aoi_ids in aoi_ids_by_type.items():
        names.update(EMAIL_AOI_TYPES[aoi_type].read_names(aoi_ids))
    return names


def admin_aoi_names(aois: pd.DataFrame) -> dict[str, str]:
    """Names of GADM AOIs, from the AOI itself up to its country, e.g. "Brazil",
    "Paraná, Brazil" or "Abatiá, Paraná, Brazil"."""
    name_columns = {0: ["country"], 1: ["name_1", "country"]}
    names = {}
    for aoi in aois.itertuples():
        columns = name_columns.get(aoi.adm_level, ["name_2", "name_1", "country"])
        names[aoi.aoi_id] = ", ".join(getattr(aoi, column) for column in columns)
    return names


def format_area(area_ha: float) -> str:
    """An area with its unit: thousands of hectares to one decimal (e.g.
    "48.0 kha") from 1,000 ha up, else whole hectares (e.g. "860 ha"), or
    "<1 ha" for a non-zero area under 1 ha."""
    if 0 < area_ha < 1:
        return "<1 ha"
    if round(area_ha) >= 1000:
        return f"{area_ha / 1000:,.1f} kha"
    return f"{round(area_ha)} ha"


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
            "total_area": format_area(sum(d["total_ha"] for d in ranked)),
            "dashboard_count": len(ranked),
            "more_dashboard_count": len(ranked) - len(shown),
            "dashboards_url": DASHBOARDS_URL,
            "dashboards": [
                {
                    "dashboard_id": d["dashboard_id"],
                    "name": d["name"],
                    "url": DASHBOARD_URL.format(dashboard_id=d["dashboard_id"]),
                    "total_area": format_area(d["total_ha"]),
                    "natural_lands_area": format_area(d["natural_lands_ha"]),
                    "natural_forest_area": format_area(d["natural_forest_ha"]),
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


def sent_marker_uri(week_end: date, email: str, aoi_type: str) -> str:
    """Return the uri that marks a subscriber's dashboards of one AOI type as
    sent for a week. The email is hashed to keep addresses out of S3 keys."""
    email_hash = hashlib.sha256(email.encode("utf-8")).hexdigest()[:16]
    return f"{SENT_MARKERS_PREFIX}/{week_end.isoformat()}/{email_hash}-{aoi_type}.json"


def _list_keys(bucket: str, prefix: str) -> list[str]:
    pages = (
        boto3.client("s3")
        .get_paginator("list_objects_v2")
        .paginate(Bucket=bucket, Prefix=prefix)
    )
    return [obj["Key"] for page in pages for obj in page.get("Contents", [])]


def list_sent_markers(week_end: date) -> set[str]:
    """List the uris of a week's sent markers."""
    bucket, prefix = parse_s3_uri(f"{SENT_MARKERS_PREFIX}/{week_end.isoformat()}/")
    return {f"s3://{bucket}/{key}" for key in _list_keys(bucket, prefix)}


def select_unsent_subscriptions(
    subscriptions: list[Subscription], week_end: date, sent_markers: set[str]
) -> list[Subscription]:
    """Select the subscriptions whose dashboards weren't yet sent for the week."""
    return [
        s
        for s in subscriptions
        if sent_marker_uri(week_end, s.user_email, s.aoi_type) not in sent_markers
    ]


def mark_sent(week_end: date, email: str, aoi_types: list[str], results: dict) -> None:
    """Record that a week's dashboards of these AOI types were sent to email."""
    s3_client = boto3.client("s3")
    for aoi_type in aoi_types:
        bucket, key = parse_s3_uri(sent_marker_uri(week_end, email, aoi_type))
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(results).encode("utf-8"),
            ContentType="application/json",
        )


def old_sent_marker_keys(keys: list[str], today: date) -> list[str]:
    """The sent marker keys (.../sent/{week_end}/{hash}-{aoi_type}.json) of
    weeks that ended more than SENT_MARKER_DAYS days before today."""
    cutoff = today - timedelta(days=SENT_MARKER_DAYS)
    return [key for key in keys if date.fromisoformat(key.split("/")[-2]) < cutoff]


def delete_old_sent_markers(today: date) -> int:
    """Delete the sent markers of weeks that ended more than SENT_MARKER_DAYS days
    before today, and return how many were deleted."""
    bucket, prefix = parse_s3_uri(f"{SENT_MARKERS_PREFIX}/")
    s3_client = boto3.client("s3")
    old_keys = old_sent_marker_keys(_list_keys(bucket, prefix), today)
    # delete_objects takes at most 1000 keys at a time.
    for start in range(0, len(old_keys), 1000):
        batch = old_keys[start:][:1000]
        s3_client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": key} for key in batch]}
        )
    return len(old_keys)
