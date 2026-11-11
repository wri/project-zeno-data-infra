import pytest
from shapely.geometry import box, mapping

from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
)
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn


@pytest.fixture()
def base_config():
    """Base configuration for model instances."""
    analytics_in = TreeCoverAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
        canopy_cover=30,
        forest_filter=None,
    )
    return analytics_in


@pytest.fixture()
def feature_collection_aoi():
    return CustomAreaOfInterest(
        feature_collection={
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "my_feature",
                    "geometry": mapping(box(-55.1, -7.1, -55.0, -7.0)),
                }
            ],
        }
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
        with pytest.raises(ValueError):
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
        with pytest.raises(ValueError):
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

    def test_natural_forest_only_is_tree_cover_loss_only(self):
        # natural_forest_only is only implemented for tree cover loss, so it must
        # not be accepted (and silently treated as primary forest) here.
        with pytest.raises(ValueError):
            TreeCoverAnalyticsIn(
                aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
                canopy_cover=15,
                forest_filter="natural_forest_only",
            )

    def test_custom_area_is_accepted(self, feature_collection_aoi):
        model = TreeCoverAnalyticsIn(aoi=feature_collection_aoi, canopy_cover=15)

        assert model.aoi.type == "feature_collection"

    @pytest.mark.parametrize("forest_filter", ["intact_forest", "natural_forest"])
    def test_forest_filters_without_precomputed_data_are_rejected(self, forest_filter):
        # The precomputed tables only carry primary forest, so other filters
        # would return primary forest or unfiltered numbers.
        with pytest.raises(ValueError):
            TreeCoverAnalyticsIn(
                aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
                canopy_cover=15,
                forest_filter=forest_filter,
            )
