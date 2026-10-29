from datetime import date

import pandas as pd
import pytest

from pipelines.subscription_emails import stages
from pipelines.zeno_api import Subscription


def _subscription(email, dashboard_id, aoi_id, aoi_type="admin"):
    return Subscription(
        subscription_id=f"sub-{dashboard_id}",
        dashboard_id=dashboard_id,
        user_id=f"user-{email}",
        user_email=email,
        aoi_type=aoi_type,
        aoi_id=aoi_id,
        dataset="integrated_alerts",
    )


def _areas(rows):
    return pd.DataFrame(
        rows,
        columns=["aoi_id", "total_ha", "natural_lands_ha", "natural_forest_ha"],
    )


WEEK = (date(2026, 9, 22), date(2026, 9, 28))
LATEST_UPDATE = date(2026, 10, 4)


@pytest.mark.parametrize(
    "alerts_date, week_end",
    [
        (date(2026, 10, 25), date(2026, 10, 20)),  # Sunday version
        (date(2026, 10, 26), date(2026, 10, 20)),  # Monday version
        (date(2026, 10, 24), date(2026, 10, 20)),  # Saturday: 4 days after
        (date(2026, 10, 23), date(2026, 10, 13)),  # Friday: too soon after 10/20
    ],
)
def test_latest_ready_week_end_is_tuesday_far_enough_before(alerts_date, week_end):
    assert stages.latest_ready_week_end(alerts_date) == week_end


def test_next_week_end_is_latest_ready_week_on_first_run():
    assert stages.next_week_end(None, date(2026, 10, 25)) == date(2026, 10, 20)


def test_next_week_end_follows_last_sent_week():
    assert stages.next_week_end(date(2026, 10, 13), date(2026, 10, 25)) == date(
        2026, 10, 20
    )


def test_next_week_end_catches_up_one_week_at_a_time():
    assert stages.next_week_end(date(2026, 9, 29), date(2026, 10, 25)) == date(
        2026, 10, 6
    )


@pytest.mark.parametrize("alerts_date", [date(2026, 10, 25), date(2026, 10, 30)])
def test_next_week_end_is_none_until_next_week_is_ready(alerts_date):
    # The week ending 10/20 was sent; the next ends 10/27, ready from 10/31.
    assert stages.next_week_end(date(2026, 10, 20), alerts_date) is None


def test_week_range_is_seven_days_ending_on_end_date():
    assert stages.week_range(date(2026, 9, 28)) == WEEK


def test_email_subscriptions_keeps_only_admin_integrated_alerts():
    admin = _subscription("a@x.org", "d1", "BRA.16")
    protected_area = _subscription("a@x.org", "d2", "555", aoi_type="protected_area")

    assert stages.email_subscriptions([admin, protected_area]) == (
        [admin],
        [protected_area],
    )


def test_summarize_alert_areas_splits_natural_lands_and_forest():
    alerts = pd.DataFrame(
        {
            "aoi_id": ["BRA.16"] * 5 + ["PER.17"],
            # 2 is natural forest, 3 other natural lands, 14 and 0 outside.
            "natural_lands_class": [2, 2, 3, 14, 0, 9],
            "area_ha": [1.0, 2.0, 4.0, 8.0, 16.0, 5.0],
        }
    )

    areas = stages.summarize_alert_areas(alerts)

    pd.testing.assert_frame_equal(
        areas,
        _areas([["BRA.16", 31.0, 7.0, 3.0], ["PER.17", 5.0, 5.0, 5.0]]),
    )


def test_aoi_names_go_from_aoi_up_to_country():
    aois = pd.DataFrame(
        {
            "aoi_id": ["BRA", "BRA.16", "BRA.16.1"],
            "adm_level": [0, 1, 2],
            "country": ["Brazil"] * 3,
            "name_1": [None, "Paraná", "Paraná"],
            "name_2": [None, None, "Abatiá"],
        }
    )

    assert stages.aoi_names(aois) == {
        "BRA": "Brazil",
        "BRA.16": "Paraná, Brazil",
        "BRA.16.1": "Abatiá, Paraná, Brazil",
    }


@pytest.mark.parametrize(
    "area_ha, expected",
    [(0, "0"), (0.4, "<1"), (0.99, "<1"), (1.4, "1"), (12345.6, "12,346")],
)
def test_format_ha(area_ha, expected):
    assert stages.format_ha(area_ha) == expected


@pytest.mark.parametrize(
    "start, end, expected",
    [
        (date(2026, 9, 22), date(2026, 9, 28), "22 – 28 Sep 2026"),
        (date(2026, 9, 28), date(2026, 10, 4), "28 Sep – 4 Oct 2026"),
        (date(2026, 12, 29), date(2027, 1, 4), "29 Dec 2026 – 4 Jan 2027"),
    ],
)
def test_format_week(start, end, expected):
    assert stages.format_week(start, end) == expected


def test_build_emails_matches_sample_email():
    subscriptions = [
        _subscription("a@x.org", "d1", "PER.17"),
        _subscription("a@x.org", "d2", "BRA.16"),
        _subscription("b@x.org", "d3", "PER.17"),
    ]
    areas = _areas([["BRA.16", 128.0, 96.0, 57.0], ["PER.17", 34.0, 32.0, 30.0]])
    names = {"BRA.16": "Paraná, Brazil", "PER.17": "Madre de Dios, Peru"}

    emails = stages.build_emails(subscriptions, areas, names, *WEEK, LATEST_UPDATE)

    assert emails["a@x.org"] == {
        "week_label": "22 – 28 Sep 2026",
        "latest_update": "4 Oct 2026",
        "total_ha": "162",
        "dashboard_count": 2,
        "more_dashboard_count": 0,
        "dashboards": [
            {
                "dashboard_id": "d2",
                "name": "Paraná, Brazil",
                "total_ha": "128",
                "natural_lands_ha": "96",
                "natural_forest_ha": "57",
                "bar_natural_forest_pct": 45,
                "bar_other_natural_pct": 30,
                "bar_outside_pct": 25,
            },
            {
                "dashboard_id": "d1",
                "name": "Madre de Dios, Peru",
                "total_ha": "34",
                "natural_lands_ha": "32",
                "natural_forest_ha": "30",
                # Scaled to Paraná's total, the longest bar.
                "bar_natural_forest_pct": 23,
                "bar_other_natural_pct": 2,
                "bar_outside_pct": 2,
            },
        ],
    }
    # b's only dashboard is scaled to its own total.
    (dashboard,) = emails["b@x.org"]["dashboards"]
    assert dashboard["bar_natural_forest_pct"] == 88


def test_build_emails_leaves_out_dashboards_and_subscribers_without_alerts():
    subscriptions = [
        _subscription("a@x.org", "d1", "BRA.16"),
        _subscription("a@x.org", "d2", "PER.17"),
        _subscription("b@x.org", "d3", "PER.17"),
        _subscription("c@x.org", "d4", "COL.1"),
    ]
    areas = _areas([["BRA.16", 5.0, 5.0, 5.0], ["PER.17", 0.0, 0.0, 0.0]])

    emails = stages.build_emails(subscriptions, areas, {}, *WEEK, LATEST_UPDATE)

    assert list(emails) == ["a@x.org"]
    assert [d["dashboard_id"] for d in emails["a@x.org"]["dashboards"]] == ["d1"]


def test_build_emails_shows_top_dashboards_and_counts_the_rest():
    count = stages.MAX_DASHBOARDS + 2
    subscriptions = [
        _subscription("a@x.org", f"d{i}", f"AOI.{i}") for i in range(count)
    ]
    areas = _areas([[f"AOI.{i}", float(i + 1), 0.0, 0.0] for i in range(count)])

    email = stages.build_emails(subscriptions, areas, {}, *WEEK, LATEST_UPDATE)[
        "a@x.org"
    ]

    assert email["dashboard_count"] == count
    assert email["more_dashboard_count"] == 2
    assert email["total_ha"] == str(sum(range(1, count + 1)))
    assert [d["total_ha"] for d in email["dashboards"]] == [
        str(i) for i in range(count, 2, -1)
    ]


def test_sent_marker_uri_hides_email_address():
    uri = stages.sent_marker_uri(date(2026, 10, 20), "a@x.org")

    assert uri.startswith(
        "s3://lcl-analytics/zonal-statistics/integrated-alerts/"
        "subscription-emails/2026-10-20/"
    )
    assert "a@x.org" not in uri
