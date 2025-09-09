import pytest
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.carbon_flux import CarbonFluxAnalyticsIn


@pytest.fixture()
def base_config():
    """Base configuration for model instances."""
    return CarbonFluxAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]), canopy_cover=30
    )


class TestCarbonFluxAnalyticsIn:
    def test_thumbprint_is_same_for_same_fields(self, base_config):
        original_thumb = base_config.thumbprint()
        model = CarbonFluxAnalyticsIn(**base_config.model_dump())
        assert model.thumbprint() == original_thumb

    def test_thumbprint_changes_when_aoi_changes(self, base_config):
        model = CarbonFluxAnalyticsIn(**base_config.model_dump())
        model.aoi = AdminAreaOfInterest(
            type="admin",
            ids=["BRA.12"],
        )

        assert model.thumbprint() != base_config.thumbprint()
