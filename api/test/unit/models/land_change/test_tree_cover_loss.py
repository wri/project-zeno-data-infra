import pytest

from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


@pytest.fixture()
def base_config():
    """Base configuration for model instances."""
    return TreeCoverLossAnalyticsIn(
        aoi={"type": "admin", "ids": ["BRA.12.1"]},
        start_year="2020",
        end_year="2023",
        canopy_cover=30,
        forest_filter="primary_forest",
        intersections=["driver"],  # Replace with actual enum
    )


class TestTreeCoverLossAnalyticsIn:
    def test_thumbprint_is_same_for_same_fields(self, base_config):
        original_thumbprint = base_config.thumbprint()
        model = TreeCoverLossAnalyticsIn(**base_config.model_dump())
        assert model.thumbprint() == original_thumbprint

    def test_thumbprint_changes_when_aoi_changes(self, base_config):
        model = TreeCoverLossAnalyticsIn(**base_config.model_dump())
        model.aoi = {"type": "admin", "ids": ["BRA.12"]}

        assert model.thumbprint() != base_config.thumbprint()

    def test_thumbprint_changes_when_start_year_changes(self, base_config):
        model = TreeCoverLossAnalyticsIn(**base_config.model_dump())
        model.start_year = "2021"

        assert model.thumbprint() != base_config.thumbprint()

    def test_thumbprint_changes_when_end_year_changes(self, base_config):
        model = TreeCoverLossAnalyticsIn(**base_config.model_dump())
        model.end_year = "2022"

        assert model.thumbprint() != base_config.thumbprint()

    def test_thumbprint_changes_when_canopy_cover_changes(self, base_config):
        model = TreeCoverLossAnalyticsIn(**base_config.model_dump())
        model.canopy_cover = 50

        assert model.thumbprint() != base_config.thumbprint()

    @pytest.mark.xfail
    def test_thumbprint_changes_when_forest_filter_changes(self, base_config):
        model = TreeCoverLossAnalyticsIn(**base_config.model_dump())
        model.forest_filter = "primary_forest"

        assert model.thumbprint() != base_config.thumbprint()

    def test_thumbprint_changes_when_intersections_changes(self, base_config):
        model = TreeCoverLossAnalyticsIn(**base_config.model_dump())
        model.intersections = []

        assert model.thumbprint() != base_config.thumbprint()

    def test_natural_forest_no_canopy_cover(self, base_config):
        TreeCoverLossAnalyticsIn(
            aoi={"type": "protected_area", "ids": ["9823"]},
            start_year="2021",
            end_year="2024",
            forest_filter="natural_forest",
            intersections=["driver"],
        )

    def test_natural_forest_with_canopy_cover_error(self, base_config):
        with pytest.raises(ValueError):
            TreeCoverLossAnalyticsIn(
                aoi={"type": "protected_area", "ids": ["9823"]},
                start_year="2021",
                end_year="2024",
                canopy_cover=30,
                forest_filter="natural_forest",
                intersections=["driver"],
            )

    def test_natural_forest_pre_2021_error(self, base_config):
        with pytest.raises(ValueError):
            TreeCoverLossAnalyticsIn(
                aoi={"type": "protected_area", "ids": ["9823"]},
                start_year="2020",
                end_year="2024",
                forest_filter="natural_forest",
                intersections=["driver"],
            )

    def test_natural_forest_admin_error(self, base_config):
        with pytest.raises(ValueError):
            TreeCoverLossAnalyticsIn(
                aoi={"type": "admin", "ids": ["IDN"]},
                start_year="2020",
                end_year="2024",
                forest_filter="natural_forest",
                intersections=["driver"],
            )
