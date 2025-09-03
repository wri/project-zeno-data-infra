import uuid

import pytest

from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn


@pytest.fixture()
def base_config():
    """Base configuration for model instances."""
    return TreeCoverAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
        canopy_cover=30,
        intersections=[],
        forest_filter=None,
    )


class TestTreeCoverAnalyticsIn:
    def test_thumbprint_is_same_for_same_fields(self, base_config):
        model = TreeCoverAnalyticsIn(**base_config.model_dump())
        assert model.thumbprint() == uuid.UUID("1880b4ab-c1da-5b32-9c9f-4ce96a6edb81")

    def test_thumbprint_changes_when_aoi_changes(self, base_config):
        model = TreeCoverAnalyticsIn(**base_config.model_dump())
        model.aoi = AdminAreaOfInterest(
            type="admin",
            ids=["BRA.12"],
        )

        assert model.thumbprint() != base_config.thumbprint()

    def test_thumbprint_changes_when_cc_changes(self, base_config):
        model = TreeCoverAnalyticsIn(**base_config.model_dump())
        model.canopy_cover = 15

        assert model.thumbprint() != base_config.thumbprint()


class TestTreeCoverAnalyticsInValidations:
    def test_tcd_must_be_valid(self):
        with pytest.raises(ValueError):
            TreeCoverAnalyticsIn(
                aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
                canopy_cover=5,
                intersections=[],
            )

    def test_valid_tcds_work(self):
        TreeCoverAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
            canopy_cover=15,
            intersections=[],
        )
