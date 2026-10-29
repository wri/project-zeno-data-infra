from app.models.land_change.land_ghg_inventory import LandGHGInventoryAnalytics


def test_result_without_a_status_yet_is_reported_as_still_initializing():
    analytics = LandGHGInventoryAnalytics(
        result=None, metadata={}, message="Resource is initializing", status=None
    )

    assert analytics.status is None
