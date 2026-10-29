from app.models.land_change.grasslands import GrasslandsAnalytics


def test_result_without_a_status_yet_is_reported_as_still_initializing():
    analytics = GrasslandsAnalytics(
        result=None, metadata={}, message="Resource is initializing", status=None
    )

    assert analytics.status is None
