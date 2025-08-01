from app.models.land_change.tree_cover import TreeCoverAnalyticsIn
from app.use_cases.analysis.tree_cover.tree_cover_service import TreeCoverService


def test_create_query_from_analytics_iso_only():
    analytics_in = TreeCoverAnalyticsIn(
        aoi={
            "type": "admin",
            "ids": ["IDN", "BRA", "COD"],
        },
        canopy_cover=30,
        forest_filter="primary_forest",
    )

    service = TreeCoverService()
    query = service.create_query_from_analytics(analytics_in)
    assert (
        query
        == "SELECT iso, SUM(umd_tree_cover_extent_2000__ha) AS umd_tree_cover_extent_2000__ha FROM data WHERE umd_tree_cover_density_2000__threshold = 30 AND iso in ('IDN', 'BRA', 'COD') AND is__umd_regional_primary_forest_2001 = true GROUP BY iso"
    )
