import pytest

from app.models.land_change.dist_alerts import (
    DistAlertsAnalytics,
    DistAlertsAnalyticsIn,
)


def test_admin_areas_below_level_2_are_rejected():
    with pytest.raises(ValueError, match="admin level allowed is 2"):
        DistAlertsAnalyticsIn(
            aoi={"type": "admin", "ids": ["IDN.24.2.1"]},
            start_date="2024-01-01",
            end_date="2024-12-31",
            intersections=[],
        )


def test_result_without_a_status_yet_is_reported_as_still_initializing():
    analytics = DistAlertsAnalytics(
        result=None, metadata={}, message="Resource is initializing", status=None
    )

    assert analytics.status is None
