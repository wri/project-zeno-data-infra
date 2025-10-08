import pytest
from pydantic_core import ValidationError

from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn


@pytest.fixture()
def base_config():
    """Base configuration for model instances."""
    return TreeCoverAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
        canopy_cover=30,
        forest_filter=None,
    )


class TestTreeCoverAnalyticsIn:
    def test_thumbprint_is_same_for_same_fields(self, base_config):
        original_thumb = base_config.thumbprint()
        model = TreeCoverAnalyticsIn(**base_config.model_dump())

        assert model.thumbprint() == original_thumb

    def test_thumbprint_changes_when_aoi_changes(self, base_config):
        model = TreeCoverAnalyticsIn(**base_config.model_dump())
        model.aoi = AdminAreaOfInterest(
            type="admin",
            ids=["IDN.12.1"],
        )

        assert model.thumbprint() != base_config.thumbprint()

    def test_thumbprint_changes_when_cc_changes(self, base_config):
        model = TreeCoverAnalyticsIn(**base_config.model_dump())
        model.canopy_cover = 15

        assert model.thumbprint() != base_config.thumbprint()

    def test_thumbprint_changes_when_ff_changes(self, base_config):
        model = TreeCoverAnalyticsIn(**base_config.model_dump())
        model.forest_filter = "primary_forest"

        assert model.thumbprint() != base_config.thumbprint()


class TestTreeCoverAnalyticsInValidations:
    def test_tcd_must_be_valid(self):
        with pytest.raises(ValidationError):
            TreeCoverAnalyticsIn(
                aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
                canopy_cover=5,
            )

    def test_valid_tcds_work(self):
        TreeCoverAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
            canopy_cover=15,
        )

    def test_forest_filter_must_be_valid(self):
        with pytest.raises(ValidationError):
            TreeCoverAnalyticsIn(
                aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
                canopy_cover=15,
                forest_filter="lizard",
            )

    def test_valid_forest_filters_work(self):
        TreeCoverAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
            canopy_cover=15,
            forest_filter="primary_forest",
        )
