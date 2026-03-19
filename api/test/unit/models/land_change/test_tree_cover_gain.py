import pytest

from app.domain.models.environment import Environment
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.tree_cover_gain import TreeCoverGainAnalyticsIn


@pytest.fixture()
def base_config():
    """Base configuration for model instances."""
    analytics_in = TreeCoverGainAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
        start_year="2015",
        end_year="2020",
    )
    analytics_in.set_input_uris(Environment.production)
    return analytics_in


class TestTreeCoverGainAnalyticsIn:
    def test_thumbprint_is_same_for_same_fields(self, base_config):
        original_thumbprint = base_config.thumbprint()

        model = TreeCoverGainAnalyticsIn(**base_config.model_dump())
        model.set_input_uris(Environment.production)

        assert model.thumbprint() == original_thumbprint

    def test_thumbprint_changes_when_aoi_changes(self, base_config):
        model = TreeCoverGainAnalyticsIn(**base_config.model_dump())
        model.set_input_uris(Environment.production)

        model.aoi = AdminAreaOfInterest(
            type="admin",
            ids=["BRA.12"],
        )

        assert model.thumbprint() != base_config.thumbprint()

    def test_thumbprint_changes_when_start_year_changes(self, base_config):
        model = TreeCoverGainAnalyticsIn(**base_config.model_dump())
        model.set_input_uris(Environment.production)

        model.start_year = "2010"

        assert model.thumbprint() != base_config.thumbprint()

    def test_thumbprint_changes_when_end_year_changes(self, base_config):
        model = TreeCoverGainAnalyticsIn(**base_config.model_dump())
        model.set_input_uris(Environment.production)

        model.end_year = "2025"

        assert model.thumbprint() != base_config.thumbprint()


class TestTreeCoverGainAnalyticsInValidations:
    def test_year_cannot_be_less_than_2000(self):
        with pytest.raises(ValueError):
            TreeCoverGainAnalyticsIn(
                aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
                start_year="1999",
                end_year="2025",
            )

    def test_year_can_be_2000(self):
        TreeCoverGainAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
            start_year="2000",
            end_year="2025",
        )

    def test_end_year_must_not_be_equal_to_start_year(self):
        with pytest.raises(ValueError):
            TreeCoverGainAnalyticsIn(
                aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
                start_year="2005",
                end_year="2005",
            )

    def test_start_year_must_not_be_multiple_of_five_starting_at_2000(self):
        with pytest.raises(ValueError):
            TreeCoverGainAnalyticsIn(
                aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
                start_year="2001",
                end_year="2005",
            )
