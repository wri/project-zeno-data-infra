import pytest
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn
from app.use_cases.analysis.tree_cover.tree_cover_service import TreeCoverService


@pytest.fixture
def analytics_in():
    return TreeCoverAnalyticsIn(
        aoi={
            "type": "admin",
            "ids": ["IDN.24.9"],
        },
        canopy_cover=30,
        forest_filter="primary_forest",
    )


def test_create_query_from_analytics(analytics_in):
    service = TreeCoverService()
    query = service.create_query_from_analytics(analytics_in)
    assert query.dataset == "tree_cover"
    assert any(
        f.field == "admin" and f.op == "in" and f.value == ["IDN.24.9"]
        for f in query.filters
    )
    assert any(
        f.field == "primary_forest" and f.op == "=" and f.value is True
        for f in query.filters
    )
    assert any(
        f.field == "canopy_cover" and f.op == ">" and f.value == 30
        for f in query.filters
    )
