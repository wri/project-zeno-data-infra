import uuid

import pytest
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.carbon_flux import CarbonFluxAnalyticsIn


@pytest.fixture()
def base_config():
    """Base configuration for model instances."""
    return CarbonFluxAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
    )


class TestCarbonFluxAnalyticsIn:
    def test_thumbprint_is_same_for_same_fields(self, base_config):
        model = CarbonFluxAnalyticsIn(**base_config.model_dump())
        assert model.thumbprint() == uuid.UUID("4fa371fa-5a11-5f75-9822-c6533d399f42")

    def test_thumbprint_changes_when_aoi_changes(self, base_config):
        model = CarbonFluxAnalyticsIn(**base_config.model_dump())
        model.aoi = AdminAreaOfInterest(
            type="admin",
            ids=["BRA.12"],
        )

        assert model.thumbprint() != base_config.thumbprint()
