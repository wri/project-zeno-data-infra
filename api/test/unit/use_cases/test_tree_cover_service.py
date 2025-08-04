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
        query[0]
        == "SELECT iso, SUM(umd_tree_cover_extent_2000__ha) AS umd_tree_cover_extent_2000__ha FROM data WHERE umd_tree_cover_density_2000__threshold = 30 AND iso in ('IDN', 'BRA', 'COD') AND is__umd_regional_primary_forest_2001 = true GROUP BY iso"
    )


def test_create_query_from_analytics_adm1_only():
    analytics_in = TreeCoverAnalyticsIn(
        aoi={
            "type": "admin",
            "ids": ["IDN.12", "BRA.1", "COD.5"],
        },
        canopy_cover=30,
        forest_filter="primary_forest",
    )

    service = TreeCoverService()
    query = service.create_query_from_analytics(analytics_in)
    assert (
        query[1]
        == "SELECT iso, adm1, SUM(umd_tree_cover_extent_2000__ha) AS umd_tree_cover_extent_2000__ha FROM data WHERE umd_tree_cover_density_2000__threshold = 30 AND (iso, adm1) in (('IDN', 12), ('BRA', 1), ('COD', 5)) AND is__umd_regional_primary_forest_2001 = true GROUP BY iso, adm1"
    )


def test_create_query_from_analytics_adm2_only():
    analytics_in = TreeCoverAnalyticsIn(
        aoi={
            "type": "admin",
            "ids": ["IDN.12.9", "BRA.1.12", "COD.5.15"],
        },
        canopy_cover=30,
        forest_filter="primary_forest",
    )

    service = TreeCoverService()
    query = service.create_query_from_analytics(analytics_in)
    assert (
        query[2]
        == "SELECT iso, adm1, adm2, SUM(umd_tree_cover_extent_2000__ha) AS umd_tree_cover_extent_2000__ha FROM data WHERE umd_tree_cover_density_2000__threshold = 30 AND (iso, adm1, adm2) in (('IDN', 12, 9), ('BRA', 1, 12), ('COD', 5, 15)) AND is__umd_regional_primary_forest_2001 = true GROUP BY iso, adm1, adm2"
    )
